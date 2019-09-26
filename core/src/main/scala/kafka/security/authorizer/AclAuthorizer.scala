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
package kafka.security.authorizer

import java.{lang, util}
import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.typesafe.scalalogging.Logger
import kafka.api.KAFKA_2_0_IV1
import kafka.security.authorizer.AclAuthorizer.VersionedAcls
import kafka.security.auth.{Acl, Operation, PermissionType, Resource, ResourceType}
import kafka.security.auth.{All, Allow, Alter, AlterConfigs, Delete, Deny, Describe, DescribeConfigs, Read, Write}
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk._
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.{ApiException, InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Time, SecurityUtils => JSecurityUtils}
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult, Action, AuthorizableRequestContext, AuthorizationResult, Authorizer, AuthorizerServerInfo}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.{Failure, Random, Success, Try}

object AclAuthorizer {
  // Optional override zookeeper cluster configuration where acls will be stored. If not specified,
  // acls will be stored in the same zookeeper where all other kafka broker metadata is stored.
  val ZkUrlProp = "authorizer.zookeeper.url"
  val ZkConnectionTimeOutProp = "authorizer.zookeeper.connection.timeout.ms"
  val ZkSessionTimeOutProp = "authorizer.zookeeper.session.timeout.ms"
  val ZkMaxInFlightRequests = "authorizer.zookeeper.max.in.flight.requests"

  // Semi-colon separated list of users that will be treated as super users and will have access to all the resources
  // for all actions from all hosts, defaults to no super users.
  val SuperUsersProp = "super.users"
  // If set to true when no acls are found for a resource, authorizer allows access to everyone. Defaults to false.
  val AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found"

  case class VersionedAcls(acls: Set[Acl], zkVersion: Int) {
    def exists: Boolean = zkVersion != ZkVersion.UnknownVersion
  }
  val NoAcls = VersionedAcls(Set.empty, ZkVersion.UnknownVersion)

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

class AclAuthorizer extends Authorizer with Logging {
  private[security] val authorizerLogger = Logger("kafka.authorizer.logger")
  private var superUsers = Set.empty[KafkaPrincipal]
  private var shouldAllowEveryoneIfNoAclIsFound = false
  private var zkClient: KafkaZkClient = _
  private var aclChangeListeners: Iterable[AclChangeSubscription] = Iterable.empty
  private var extendedAclSupport: Boolean = _

  @volatile
  private var aclCache = new scala.collection.immutable.TreeMap[Resource, VersionedAcls]()(AclAuthorizer.ResourceOrdering)
  private val lock = new ReentrantReadWriteLock()

  // The maximum number of times we should try to update the resource acls in zookeeper before failing;
  // This should never occur, but is a safeguard just in case.
  protected[security] var maxUpdateRetries = 10

  private val retryBackoffMs = 100
  private val retryBackoffJitterMs = 50

  /**
   * Guaranteed to be called before any authorize call is made.
   */
  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    val configs = javaConfigs.asScala
    val props = new java.util.Properties()
    configs.foreach { case (key, value) => props.put(key, value.toString) }

    superUsers = configs.get(AclAuthorizer.SuperUsersProp).collect {
      case str: String if str.nonEmpty => str.split(";").map(s => JSecurityUtils.parseKafkaPrincipal(s.trim)).toSet
    }.getOrElse(Set.empty[KafkaPrincipal])

    shouldAllowEveryoneIfNoAclIsFound = configs.get(AclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).exists(_.toString.toBoolean)

    // Use `KafkaConfig` in order to get the default ZK config values if not present in `javaConfigs`. Note that this
    // means that `KafkaConfig.zkConnect` must always be set by the user (even if `AclAuthorizer.ZkUrlProp` is also
    // set).
    val kafkaConfig = KafkaConfig.fromProps(props, doLog = false)
    val zkUrl = configs.get(AclAuthorizer.ZkUrlProp).map(_.toString).getOrElse(kafkaConfig.zkConnect)
    val zkConnectionTimeoutMs = configs.get(AclAuthorizer.ZkConnectionTimeOutProp).map(_.toString.toInt).getOrElse(kafkaConfig.zkConnectionTimeoutMs)
    val zkSessionTimeOutMs = configs.get(AclAuthorizer.ZkSessionTimeOutProp).map(_.toString.toInt).getOrElse(kafkaConfig.zkSessionTimeoutMs)
    val zkMaxInFlightRequests = configs.get(AclAuthorizer.ZkMaxInFlightRequests).map(_.toString.toInt).getOrElse(kafkaConfig.zkMaxInFlightRequests)

    val time = Time.SYSTEM
    zkClient = KafkaZkClient(zkUrl, kafkaConfig.zkEnableSecureAcls, zkSessionTimeOutMs, zkConnectionTimeoutMs,
      zkMaxInFlightRequests, time, "kafka.security", "AclAuthorizer", name=Some("ACL authorizer"))
    zkClient.createAclPaths()

    extendedAclSupport = kafkaConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1

    // Start change listeners first and then populate the cache so that there is no timing window
    // between loading cache and processing change notifications.
    startZkChangeListeners()
    loadCache()
  }

  override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
    serverInfo.endpoints.asScala.map { endpoint =>
      endpoint -> CompletableFuture.completedFuture[Void](null) }.toMap.asJava
  }

  override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
    actions.asScala.map { action => authorizeAction(requestContext, action) }.asJava
  }

  override def createAcls(requestContext: AuthorizableRequestContext,
                          aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
    val results = new Array[AclCreateResult](aclBindings.size)
    val aclsToCreate = aclBindings.asScala.zipWithIndex
      .filter { case (aclBinding, i) =>
        try {
          if (!extendedAclSupport && aclBinding.pattern.patternType == PatternType.PREFIXED) {
            throw new UnsupportedVersionException(s"Adding ACLs on prefixed resource patterns requires " +
              s"${KafkaConfig.InterBrokerProtocolVersionProp} of $KAFKA_2_0_IV1 or greater")
          }
          AuthorizerUtils.validateAclBinding(aclBinding)
          true
        } catch {
          case e: Throwable =>
            results(i) = new AclCreateResult(new InvalidRequestException("Failed to create ACL", apiException(e)))
            false
        }
      }.groupBy(_._1.pattern)

    if (aclsToCreate.nonEmpty) {
      inWriteLock(lock) {
        aclsToCreate.foreach { case (resource, aclsWithIndex) =>
          try {
            updateResourceAcls(AuthorizerUtils.convertToResource(resource)) { currentAcls =>
              val newAcls = aclsWithIndex.map { case (acl, index) => AuthorizerUtils.convertToAcl(acl.entry) }
              currentAcls ++ newAcls
            }
            aclsWithIndex.foreach { case (_, index) => results(index) = AclCreateResult.SUCCESS }
          } catch {
            case e: Throwable =>
              aclsWithIndex.foreach { case (_, index) => results(index) = new AclCreateResult(apiException(e)) }
          }
        }
      }
    }
    results.toList.map(CompletableFuture.completedFuture[AclCreateResult]).asJava
  }

  override def deleteAcls(requestContext: AuthorizableRequestContext,
                          aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
    val deletedBindings = new mutable.HashMap[AclBinding, Int]()
    val deleteExceptions = new mutable.HashMap[AclBinding, ApiException]()
    val filters = aclBindingFilters.asScala.zipWithIndex
    inWriteLock(lock) {
      val resourcesToUpdate = aclCache.keys.map { resource =>
        val matchingFilters = filters.filter { case (filter, _) =>
          filter.patternFilter.matches(resource.toPattern)
        }
        resource -> matchingFilters
      }.toMap.filter(_._2.nonEmpty)

      resourcesToUpdate.foreach { case (resource, matchingFilters) =>
        val resourceBindingsBeingDeleted = new mutable.HashMap[AclBinding, Int]()
        try {
          updateResourceAcls(resource) { currentAcls =>
            val aclsToRemove = currentAcls.filter { acl =>
              matchingFilters.exists { case (filter, index) =>
                val matches = filter.entryFilter.matches(AuthorizerUtils.convertToAccessControlEntry(acl))
                if (matches) {
                  val binding = AuthorizerUtils.convertToAclBinding(resource, acl)
                  deletedBindings.getOrElseUpdate(binding, index)
                  resourceBindingsBeingDeleted.getOrElseUpdate(binding, index)
                }
                matches
              }
            }
            currentAcls -- aclsToRemove
          }
        } catch {
          case e: Exception =>
            resourceBindingsBeingDeleted.foreach { case (binding, index) =>
                deleteExceptions.getOrElseUpdate(binding, apiException(e))
            }
        }
      }
    }
    val deletedResult = deletedBindings.groupBy(_._2)
      .mapValues(_.map{ case (binding, _) => new AclBindingDeleteResult(binding, deleteExceptions.getOrElse(binding, null)) })
    (0 until aclBindingFilters.size).map { i =>
      new AclDeleteResult(deletedResult.getOrElse(i, Set.empty[AclBindingDeleteResult]).toSet.asJava)
    }.map(CompletableFuture.completedFuture[AclDeleteResult]).asJava
  }

  override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = {
    inReadLock(lock) {
      unorderedAcls.flatMap { case (resource, versionedAcls) =>
        versionedAcls.acls.map(acl => AuthorizerUtils.convertToAclBinding(resource, acl))
            .filter(filter.matches)
      }.asJava
    }
  }

  override def close(): Unit = {
    aclChangeListeners.foreach(listener => listener.close())
    if (zkClient != null) zkClient.close()
  }

  private def authorizeAction(requestContext: AuthorizableRequestContext, action: Action): AuthorizationResult = {
    val resource = AuthorizerUtils.convertToResource(action.resourcePattern)
    if (resource.patternType != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported. Got: " + resource.patternType)
    }

    // ensure we compare identical classes
    val sessionPrincipal = requestContext.principal
    val principal = if (classOf[KafkaPrincipal] != sessionPrincipal.getClass)
      new KafkaPrincipal(sessionPrincipal.getPrincipalType, sessionPrincipal.getName)
    else
      sessionPrincipal

    val host = requestContext.clientAddress.getHostAddress
    val operation = Operation.fromJava(action.operation)

    def isEmptyAclAndAuthorized(acls: Set[Acl]): Boolean = {
      if (acls.isEmpty) {
        // No ACLs found for this resource, permission is determined by value of config allow.everyone.if.no.acl.found
        authorizerLogger.debug(s"No acl found for resource $resource, authorized = $shouldAllowEveryoneIfNoAclIsFound")
        shouldAllowEveryoneIfNoAclIsFound
      } else false
    }

    def denyAclExists(acls: Set[Acl]): Boolean = {
      // Check if there are any Deny ACLs which would forbid this operation.
      matchingAclExists(operation, resource, principal, host, Deny, acls)
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
      allowOps.exists(operation => matchingAclExists(operation, resource, principal, host, Allow, acls))
    }

    def aclsAllowAccess = {
      //we allow an operation if no acls are found and user has configured to allow all users
      //when no acls are found or if no deny acls are found and at least one allow acls matches.
      val acls = matchingAcls(resource.resourceType, resource.name)
      isEmptyAclAndAuthorized(acls) || (!denyAclExists(acls) && allowAclExists(acls))
    }

    // Evaluate if operation is allowed
    val authorized = isSuperUser(principal) || aclsAllowAccess

    logAuditMessage(requestContext, action, authorized)
    if (authorized) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
  }

  def isSuperUser(principal: KafkaPrincipal): Boolean = {
    if (superUsers.contains(principal)) {
      authorizerLogger.debug(s"principal = $principal is a super user, allowing operation without checking acls.")
      true
    } else false
  }

  private def matchingAcls(resourceType: ResourceType, resourceName: String): Set[Acl] = {
    inReadLock(lock) {
      val wildcard = aclCache.get(Resource(resourceType, Acl.WildCardResource, PatternType.LITERAL))
        .map(_.acls)
        .getOrElse(Set.empty[Acl])

      val literal = aclCache.get(Resource(resourceType, resourceName, PatternType.LITERAL))
        .map(_.acls)
        .getOrElse(Set.empty[Acl])

      val prefixed = aclCache
        .from(Resource(resourceType, resourceName, PatternType.PREFIXED))
        .to(Resource(resourceType, resourceName.take(1), PatternType.PREFIXED))
        .filterKeys(resource => resourceName.startsWith(resource.name))
        .values
        .flatMap { _.acls }
        .toSet

      prefixed ++ wildcard ++ literal
    }
  }

  private def matchingAclExists(operation: Operation, resource: Resource, principal: KafkaPrincipal, host: String, permissionType: PermissionType, acls: Set[Acl]): Boolean = {
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

  private def loadCache(): Unit = {
    inWriteLock(lock) {
      ZkAclStore.stores.foreach(store => {
        val resourceTypes = zkClient.getResourceTypes(store.patternType)
        for (rType <- resourceTypes) {
          val resourceType = Try(ResourceType.fromString(rType))
          resourceType match {
            case Success(resourceTypeObj) =>
              val resourceNames = zkClient.getResourceNames(store.patternType, resourceTypeObj)
              for (resourceName <- resourceNames) {
                val resource = new Resource(resourceTypeObj, resourceName, store.patternType)
                val versionedAcls = getAclsFromZk(resource)
                updateCache(resource, versionedAcls)
              }
            case Failure(_) => warn(s"Ignoring unknown ResourceType: $rType")
          }
        }
      })
    }
  }

  private[authorizer] def startZkChangeListeners(): Unit = {
    aclChangeListeners = ZkAclChangeStore.stores
      .map(store => store.createListener(AclChangedNotificationHandler, zkClient))
  }

  def logAuditMessage(requestContext: AuthorizableRequestContext, action: Action, authorized: Boolean): Unit = {
    def logMessage: String = {
      val principal = requestContext.principal
      val operation = Operation.fromJava(action.operation)
      val host = requestContext.clientAddress.getHostAddress
      val resource = AuthorizerUtils.convertToResource(action.resourcePattern)
      val authResult = if (authorized) "Allowed" else "Denied"
      val apiKey = if (ApiKeys.hasId(requestContext.requestType)) ApiKeys.forId(requestContext.requestType).name else requestContext.requestType
      val refCount = action.resourceReferenceCount
      s"Principal = $principal is $authResult Operation = $operation from host = $host on resource = $resource for request = $apiKey with resourceRefCount = $refCount"
    }

    if (authorized) {
      // logIfAllowed is true if access is granted to the resource as a result of this authorization.
      // In this case, log at debug level. If false, no access is actually granted, the result is used
      // only to determine authorized operations. So log only at trace level.
      if (action.logIfAllowed)
        authorizerLogger.debug(logMessage)
      else
        authorizerLogger.trace(logMessage)
    } else {
      // logIfDenied is true if access to the resource was explicitly requested. Since this is an attempt
      // to access unauthorized resources, log at info level. If false, this is either a request to determine
      // authorized operations or a filter (e.g for regex subscriptions) to filter out authorized resources.
      // In this case, log only at trace level.
      if (action.logIfDenied)
        authorizerLogger.info(logMessage)
      else
        authorizerLogger.trace(logMessage)
    }
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

  // Returns Map instead of SortedMap since most callers don't care about ordering. In Scala 2.13, mapping from SortedMap
  // to Map is restricted by default
  private def unorderedAcls: Map[Resource, VersionedAcls] = aclCache

  private def getAclsFromCache(resource: Resource): VersionedAcls = {
    aclCache.getOrElse(resource, throw new IllegalArgumentException(s"ACLs do not exist in the cache for resource $resource"))
  }

  private def getAclsFromZk(resource: Resource): VersionedAcls = {
    zkClient.getVersionedAclsForResource(resource)
  }

  private def updateCache(resource: Resource, versionedAcls: VersionedAcls): Unit = {
    if (versionedAcls.acls.nonEmpty) {
      aclCache = aclCache + (resource -> versionedAcls)
    } else {
      aclCache = aclCache - resource
    }
  }

  private def updateAclChangedFlag(resource: Resource): Unit = {
      zkClient.createAclChangeNotification(resource)
  }

  private def backoffTime = {
    retryBackoffMs + Random.nextInt(retryBackoffJitterMs)
  }

  private def apiException(e: Throwable): ApiException = {
    e match {
      case e1: ApiException => e1
      case e1 => new ApiException(e1)
    }
  }

  object AclChangedNotificationHandler extends AclChangeNotificationHandler {
    override def processNotification(resource: Resource): Unit = {
      inWriteLock(lock) {
        val versionedAcls = getAclsFromZk(resource)
        updateCache(resource, versionedAcls)
      }
    }
  }
}
