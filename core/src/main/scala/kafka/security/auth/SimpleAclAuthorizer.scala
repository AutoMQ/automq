/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.typesafe.scalalogging.Logger
import kafka.api.KAFKA_2_0_IV1
import kafka.network.RequestChannel.Session
import kafka.security.auth.SimpleAclAuthorizer.{NoAcls, VersionedAcls}
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.{AclChangeNotificationHandler, AclChangeSubscription, KafkaZkClient, ZkAclChangeStore, ZkAclStore, ZkVersion}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{SecurityUtils, Time}

import scala.collection.JavaConverters._
import scala.util.{Failure, Random, Success, Try}

object SimpleAclAuthorizer {
  //optional override zookeeper cluster configuration where acls will be stored, if not specified acls will be stored in
  //same zookeeper where all other kafka broker info is stored.
  val ZkUrlProp = "authorizer.zookeeper.url"
  val ZkConnectionTimeOutProp = "authorizer.zookeeper.connection.timeout.ms"
  val ZkSessionTimeOutProp = "authorizer.zookeeper.session.timeout.ms"
  val ZkMaxInFlightRequests = "authorizer.zookeeper.max.in.flight.requests"

  //List of users that will be treated as super users and will have access to all the resources for all actions from all hosts, defaults to no super users.
  val SuperUsersProp = "super.users"
  //If set to true when no acls are found for a resource , authorizer allows access to everyone. Defaults to false.
  val AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found"

  case class VersionedAcls(acls: Set[Acl], zkVersion: Int) {
    def exists: Boolean = zkVersion != ZkVersion.UnknownVersion
  }
  val NoAcls = VersionedAcls(Set.empty, ZkVersion.UnknownVersion)
}

class SimpleAclAuthorizer extends Authorizer with Logging {
  private val authorizerLogger = Logger("kafka.authorizer.logger")
  private var superUsers = Set.empty[KafkaPrincipal]
  private var shouldAllowEveryoneIfNoAclIsFound = false
  private var zkClient: KafkaZkClient = _
  private var aclChangeListeners: Iterable[AclChangeSubscription] = Iterable.empty
  private var extendedAclSupport: Boolean = _

  @volatile
  private var aclCache = new scala.collection.immutable.TreeMap[Resource, VersionedAcls]()(ResourceOrdering)
  private val lock = new ReentrantReadWriteLock()

  // The maximum number of times we should try to update the resource acls in zookeeper before failing;
  // This should never occur, but is a safeguard just in case.
  protected[auth] var maxUpdateRetries = 10

  private val retryBackoffMs = 100
  private val retryBackoffJitterMs = 50

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def configure(javaConfigs: util.Map[String, _]) {
    val configs = javaConfigs.asScala
    val props = new java.util.Properties()
    configs.foreach { case (key, value) => props.put(key, value.toString) }

    superUsers = configs.get(SimpleAclAuthorizer.SuperUsersProp).collect {
      case str: String if str.nonEmpty => str.split(";").map(s => SecurityUtils.parseKafkaPrincipal(s.trim)).toSet
    }.getOrElse(Set.empty[KafkaPrincipal])

    shouldAllowEveryoneIfNoAclIsFound = configs.get(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).exists(_.toString.toBoolean)

    // Use `KafkaConfig` in order to get the default ZK config values if not present in `javaConfigs`. Note that this
    // means that `KafkaConfig.zkConnect` must always be set by the user (even if `SimpleAclAuthorizer.ZkUrlProp` is also
    // set).
    val kafkaConfig = KafkaConfig.fromProps(props, doLog = false)
    val zkUrl = configs.get(SimpleAclAuthorizer.ZkUrlProp).map(_.toString).getOrElse(kafkaConfig.zkConnect)
    val zkConnectionTimeoutMs = configs.get(SimpleAclAuthorizer.ZkConnectionTimeOutProp).map(_.toString.toInt).getOrElse(kafkaConfig.zkConnectionTimeoutMs)
    val zkSessionTimeOutMs = configs.get(SimpleAclAuthorizer.ZkSessionTimeOutProp).map(_.toString.toInt).getOrElse(kafkaConfig.zkSessionTimeoutMs)
    val zkMaxInFlightRequests = configs.get(SimpleAclAuthorizer.ZkMaxInFlightRequests).map(_.toString.toInt).getOrElse(kafkaConfig.zkMaxInFlightRequests)

    val time = Time.SYSTEM
    zkClient = KafkaZkClient(zkUrl, kafkaConfig.zkEnableSecureAcls, zkSessionTimeOutMs, zkConnectionTimeoutMs,
      zkMaxInFlightRequests, time, "kafka.security", "SimpleAclAuthorizer")
    zkClient.createAclPaths()

    extendedAclSupport = kafkaConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1

    // Start change listeners first and then populate the cache so that there is no timing window
    // between loading cache and processing change notifications.
    startZkChangeListeners()
    loadCache()
  }

  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    if (resource.patternType != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported. Got: " + resource.patternType)
    }

    // ensure we compare identical classes
    val sessionPrincipal = session.principal
    val principal = if (classOf[KafkaPrincipal] != sessionPrincipal.getClass)
      new KafkaPrincipal(sessionPrincipal.getPrincipalType, sessionPrincipal.getName)
    else
      sessionPrincipal

    val host = session.clientAddress.getHostAddress

    def isEmptyAclAndAuthorized(acls: Set[Acl]): Boolean = {
      if (acls.isEmpty) {
        // No ACLs found for this resource, permission is determined by value of config allow.everyone.if.no.acl.found
        authorizerLogger.debug(s"No acl found for resource $resource, authorized = $shouldAllowEveryoneIfNoAclIsFound")
        shouldAllowEveryoneIfNoAclIsFound
      } else false
    }

    def denyAclExists(acls: Set[Acl]): Boolean = {
      // Check if there are any Deny ACLs which would forbid this operation.
      aclMatch(operation, resource, principal, host, Deny, acls)
    }

    def allowAclExists(acls: Set[Acl]): Boolean = {
      // Check if there are any Allow ACLs which would allow this operation.
      // Allowing read, write, delete, or alter implies allowing describe.
      // See #{org.apache.kafka.common.acl.AclOperation} for more details about ACL inheritance.
      val allowOps = operation match {
        case Describe => Set[Operation](Describe, Read, Write, Delete, Alter)
        case DescribeConfigs => Set[Operation](DescribeConfigs, AlterConfigs)
        case _ => Set[Operation](operation)
      }
      allowOps.exists(operation => aclMatch(operation, resource, principal, host, Allow, acls))
    }

    def aclsAllowAccess = {
      //we allow an operation if no acls are found and user has configured to allow all users
      //when no acls are found or if no deny acls are found and at least one allow acls matches.
      val acls = getMatchingAcls(resource.resourceType, resource.name)
      isEmptyAclAndAuthorized(acls) || (!denyAclExists(acls) && allowAclExists(acls))
    }

    // Evaluate if operation is allowed
    val authorized = isSuperUser(operation, resource, principal, host) || aclsAllowAccess

    logAuditMessage(principal, authorized, operation, resource, host)
    authorized
  }

  def isSuperUser(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String): Boolean = {
    if (superUsers.contains(principal)) {
      authorizerLogger.debug(s"principal = $principal is a super user, allowing operation without checking acls.")
      true
    } else false
  }

  private def aclMatch(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
    acls.find { acl =>
      acl.permissionType == permissionType &&
        (acl.principal == principal || acl.principal == Acl.WildCardPrincipal) &&
        (operation == acl.operation || acl.operation == All) &&
        (acl.host == host || acl.host == Acl.WildCardHost)
    }.exists { acl =>
      authorizerLogger.debug(s"operation = $operation on resource = $resource from host = $host is $permissionType based on acl = $acl")
      true
    }
  }

  override def addAcls(acls: Set[Acl], resource: Resource) {
    if (acls != null && acls.nonEmpty) {
      if (!extendedAclSupport && resource.patternType == PatternType.PREFIXED) {
        throw new UnsupportedVersionException(s"Adding ACLs on prefixed resource patterns requires " +
          s"${KafkaConfig.InterBrokerProtocolVersionProp} of $KAFKA_2_0_IV1 or greater")
      }

      inWriteLock(lock) {
        updateResourceAcls(resource) { currentAcls =>
          currentAcls ++ acls
        }
      }
    }
  }

  override def removeAcls(aclsTobeRemoved: Set[Acl], resource: Resource): Boolean = {
    inWriteLock(lock) {
      updateResourceAcls(resource) { currentAcls =>
        currentAcls -- aclsTobeRemoved
      }
    }
  }

  override def removeAcls(resource: Resource): Boolean = {
    inWriteLock(lock) {
      val result = zkClient.deleteResource(resource)
      updateCache(resource, NoAcls)
      updateAclChangedFlag(resource)
      result
    }
  }

  override def getAcls(resource: Resource): Set[Acl] = {
    inReadLock(lock) {
      aclCache.get(resource).map(_.acls).getOrElse(Set.empty[Acl])
    }
  }

  override def getAcls(principal: KafkaPrincipal): Map[Resource, Set[Acl]] = {
    inReadLock(lock) {
      aclCache.mapValues { versionedAcls =>
        versionedAcls.acls.filter(_.principal == principal)
      }.filter { case (_, acls) =>
        acls.nonEmpty
      }
    }
  }

  def getMatchingAcls(resourceType: ResourceType, resourceName: String): Set[Acl] = {
    inReadLock(lock) {
      val wildcard = aclCache.get(Resource(resourceType, Acl.WildCardResource, PatternType.LITERAL))
        .map(_.acls)
        .getOrElse(Set.empty[Acl])

      val literal = aclCache.get(Resource(resourceType, resourceName, PatternType.LITERAL))
        .map(_.acls)
        .getOrElse(Set.empty[Acl])

      val prefixed = aclCache.range(
        Resource(resourceType, resourceName, PatternType.PREFIXED),
        Resource(resourceType, resourceName.take(1), PatternType.PREFIXED)
      )
        .filterKeys(resource => resourceName.startsWith(resource.name))
        .flatMap { case (resource, versionedAcls) => versionedAcls.acls }
        .toSet

      prefixed ++ wildcard ++ literal
    }
  }

  override def getAcls(): Map[Resource, Set[Acl]] = {
    inReadLock(lock) {
      aclCache.mapValues(_.acls)
    }
  }

  def close() {
    aclChangeListeners.foreach(listener => listener.close())
    if (zkClient != null) zkClient.close()
  }

  private def loadCache() {
    inWriteLock(lock) {
      ZkAclStore.stores.foreach(store => {
        val resourceTypes = zkClient.getResourceTypes(store.patternType)
        for (rType <- resourceTypes) {
          val resourceType = Try(ResourceType.fromString(rType))
          resourceType match {
            case Success(resourceTypeObj) => {
              val resourceNames = zkClient.getResourceNames(store.patternType, resourceTypeObj)
              for (resourceName <- resourceNames) {
                val resource = new Resource(resourceTypeObj, resourceName, store.patternType)
                val versionedAcls = getAclsFromZk(resource)
                updateCache(resource, versionedAcls)
              }
            }
            case Failure(f) => warn(s"Ignoring unknown ResourceType: $rType")
          }
        }
      })
    }
  }

  private[auth] def startZkChangeListeners(): Unit = {
    aclChangeListeners = ZkAclChangeStore.stores
      .map(store => store.createListener(AclChangedNotificationHandler, zkClient))
  }

  private def logAuditMessage(principal: KafkaPrincipal, authorized: Boolean, operation: Operation, resource: Resource, host: String) {
    def logMessage: String = {
      val authResult = if (authorized) "Allowed" else "Denied"
      s"Principal = $principal is $authResult Operation = $operation from host = $host on resource = $resource"
    }

    if (authorized) authorizerLogger.debug(logMessage)
    else authorizerLogger.info(logMessage)
  }

  /**
    * Safely updates the resources ACLs by ensuring reads and writes respect the expected zookeeper version.
    * Continues to retry until it successfully updates zookeeper.
    *
    * Returns a boolean indicating if the content of the ACLs was actually changed.
    *
    * @param resource the resource to change ACLs for
    * @param getNewAcls function to transform existing acls to new ACLs
    * @return boolean indicating if a change was made
    */
  private def updateResourceAcls(resource: Resource)(getNewAcls: Set[Acl] => Set[Acl]): Boolean = {
    var currentVersionedAcls =
      if (aclCache.contains(resource))
        getAclsFromCache(resource)
      else
        getAclsFromZk(resource)
    var newVersionedAcls: VersionedAcls = null
    var writeComplete = false
    var retries = 0
    while (!writeComplete && retries <= maxUpdateRetries) {
      val newAcls = getNewAcls(currentVersionedAcls.acls)
      val (updateSucceeded, updateVersion) =
        if (newAcls.nonEmpty) {
          if (currentVersionedAcls.exists)
            zkClient.conditionalSetAclsForResource(resource, newAcls, currentVersionedAcls.zkVersion)
          else
            zkClient.createAclsForResourceIfNotExists(resource, newAcls)
        } else {
          trace(s"Deleting path for $resource because it had no ACLs remaining")
          (zkClient.conditionalDelete(resource, currentVersionedAcls.zkVersion), 0)
        }

      if (!updateSucceeded) {
        trace(s"Failed to update ACLs for $resource. Used version ${currentVersionedAcls.zkVersion}. Reading data and retrying update.")
        Thread.sleep(backoffTime)
        currentVersionedAcls = getAclsFromZk(resource)
        retries += 1
      } else {
        newVersionedAcls = VersionedAcls(newAcls, updateVersion)
        writeComplete = updateSucceeded
      }
    }

    if(!writeComplete)
      throw new IllegalStateException(s"Failed to update ACLs for $resource after trying a maximum of $maxUpdateRetries times")

    if (newVersionedAcls.acls != currentVersionedAcls.acls) {
      debug(s"Updated ACLs for $resource to ${newVersionedAcls.acls} with version ${newVersionedAcls.zkVersion}")
      updateCache(resource, newVersionedAcls)
      updateAclChangedFlag(resource)
      true
    } else {
      debug(s"Updated ACLs for $resource, no change was made")
      updateCache(resource, newVersionedAcls) // Even if no change, update the version
      false
    }
  }

  private def getAclsFromCache(resource: Resource): VersionedAcls = {
    aclCache.getOrElse(resource, throw new IllegalArgumentException(s"ACLs do not exist in the cache for resource $resource"))
  }

  private def getAclsFromZk(resource: Resource): VersionedAcls = {
    zkClient.getVersionedAclsForResource(resource)
  }

  private def updateCache(resource: Resource, versionedAcls: VersionedAcls) {
    if (versionedAcls.acls.nonEmpty) {
      aclCache = aclCache + (resource -> versionedAcls)
    } else {
      aclCache = aclCache - resource
    }
  }

  private def updateAclChangedFlag(resource: Resource) {
      zkClient.createAclChangeNotification(resource)
  }

  private def backoffTime = {
    retryBackoffMs + Random.nextInt(retryBackoffJitterMs)
  }

  object AclChangedNotificationHandler extends AclChangeNotificationHandler {
    override def processNotification(resource: Resource) {
      inWriteLock(lock) {
        val versionedAcls = getAclsFromZk(resource)
        updateCache(resource, versionedAcls)
      }
    }
  }

  // Orders by resource type, then resource pattern type and finally reverse ordering by name.
  private object ResourceOrdering extends Ordering[Resource] {

    def compare(a: Resource, b: Resource): Int = {
      val rt = a.resourceType compare b.resourceType
      if (rt != 0)
        rt
      else {
        val rnt = a.patternType compareTo b.patternType
        if (rnt != 0)
          rnt
        else
          (a.name compare b.name) * -1
      }
    }
  }
}
