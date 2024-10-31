/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import kafka.security.authorizer.AclAuthorizer
import kafka.utils.TestUtils._
import kafka.utils.{JaasTestUtils, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation.{ALL, ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, DELETE, DESCRIBE, IDEMPOTENT_WRITE}
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.config.{ConfigResource, SaslConfigs, TopicConfig}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, DelegationTokenExpiredException, DelegationTokenNotFoundException, InvalidRequestException, TopicAuthorizationException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.apache.kafka.security.authorizer.AclEntry.{WILDCARD_HOST, WILDCARD_PRINCIPAL_STRING}
import org.apache.kafka.server.config.{DelegationTokenManagerConfigs, ServerConfigs, ZkConfigs}
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

@Timeout(120)
class SaslSslAdminIntegrationTest extends BaseAdminIntegrationTest with SaslSetup {
  val clusterResourcePattern = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
  val zkAuthorizerClassName = classOf[AclAuthorizer].getName
  val kraftAuthorizerClassName = classOf[StandardAuthorizer].getName
  val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaServerPrincipalUnqualifiedName)
  var superUserAdmin: Admin = _
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    if (TestInfoUtils.isKRaft(testInfo)) {
      this.serverConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, kraftAuthorizerClassName)
      this.controllerConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, kraftAuthorizerClassName)
      // controllers talk to brokers as User:ANONYMOUS therefore it needs to be super user
      this.serverConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString + ";" + KafkaPrincipal.ANONYMOUS.toString)
      this.controllerConfig.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG, kafkaPrincipal.toString)
    } else {
      this.serverConfig.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, zkAuthorizerClassName)
      this.serverConfig.setProperty(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, "true")
      this.serverConfig.setProperty(AclAuthorizer.SuperUsersProp, kafkaPrincipal.toString)
    }

    // Enable delegationTokenControlManager
    serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "123")
    serverConfig.setProperty(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG, "5000")

    setUpSasl()
    super.setUp(testInfo)
    setInitialAcls()
  }

  def setUpSasl(): Unit = {
    startSasl(jaasSections(Seq("GSSAPI"), Some("GSSAPI"), Both, JaasTestUtils.KafkaServerContextName))

    val loginContext = jaasAdminLoginModule("GSSAPI")
    superuserClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, loginContext)
  }

  private def setInitialAcls(): Unit = {
    superUserAdmin = createSuperuserAdminClient()
    val ace = new AccessControlEntry(WILDCARD_PRINCIPAL_STRING, WILDCARD_HOST, ALL, ALLOW)
    superUserAdmin.createAcls(List(new AclBinding(new ResourcePattern(TOPIC, "*", LITERAL), ace)).asJava)
    superUserAdmin.createAcls(List(new AclBinding(new ResourcePattern(GROUP, "*", LITERAL), ace)).asJava)

    val clusterAcls = List(clusterAcl(ALLOW, CREATE),
      clusterAcl(ALLOW, DELETE),
      clusterAcl(ALLOW, CLUSTER_ACTION),
      clusterAcl(ALLOW, ALTER_CONFIGS),
      clusterAcl(ALLOW, ALTER),
      clusterAcl(ALLOW, IDEMPOTENT_WRITE))

    superUserAdmin.createAcls(clusterAcls.map(ace => new AclBinding(clusterResourcePattern, ace)).asJava)

    brokers.foreach { b =>
      TestUtils.waitAndVerifyAcls(Set(ace), b.dataPlaneRequestProcessor.authorizer.get, new ResourcePattern(TOPIC, "*", LITERAL))
      TestUtils.waitAndVerifyAcls(Set(ace), b.dataPlaneRequestProcessor.authorizer.get, new ResourcePattern(GROUP, "*", LITERAL))
      TestUtils.waitAndVerifyAcls(clusterAcls.toSet, b.dataPlaneRequestProcessor.authorizer.get, clusterResourcePattern)
    }
  }

  private def clusterAcl(permissionType: AclPermissionType, operation: AclOperation): AccessControlEntry = {
    new AccessControlEntry(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*").toString,
      WILDCARD_HOST, operation, permissionType)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  val anyAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
    new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
  val acl2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val acl3 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val fooAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foobar", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val prefixAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
  val transactionalIdAcl = new AclBinding(new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "transactional_id", PatternType.LITERAL),
    new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
  val groupAcl = new AclBinding(new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL),
    new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclOperations(quorum: String): Unit = {
    client = createAdminClient
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    assertEquals(8, getAcls(AclBindingFilter.ANY).size)
    val results = client.createAcls(List(acl2, acl3).asJava)
    assertEquals(Set(acl2, acl3), results.values.keySet().asScala)
    results.values.values.forEach(value => value.get)
    val aclUnknown = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW))
    val results2 = client.createAcls(List(aclUnknown).asJava)
    assertEquals(Set(aclUnknown), results2.values.keySet().asScala)
    assertFutureExceptionTypeEquals(results2.all, classOf[InvalidRequestException])
    val results3 = client.deleteAcls(List(acl.toFilter, acl2.toFilter, acl3.toFilter).asJava).values
    assertEquals(Set(acl.toFilter, acl2.toFilter, acl3.toFilter), results3.keySet.asScala)
    assertEquals(0, results3.get(acl.toFilter).get.values.size())
    assertEquals(Set(acl2), results3.get(acl2.toFilter).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl3), results3.get(acl3.toFilter).get.values.asScala.map(_.binding).toSet)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclOperations2(quorum: String): Unit = {
    client = createAdminClient
    val results = client.createAcls(List(acl2, acl2, transactionalIdAcl).asJava)
    assertEquals(Set(acl2, acl2, transactionalIdAcl), results.values.keySet.asScala)
    results.all.get()
    waitForDescribeAcls(client, acl2.toFilter, Set(acl2))
    waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))

    val filterA = new AclBindingFilter(new ResourcePatternFilter(ResourceType.GROUP, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val filterB = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val filterC = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TRANSACTIONAL_ID, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)

    waitForDescribeAcls(client, filterA, Set(groupAcl))
    waitForDescribeAcls(client, filterC, Set(transactionalIdAcl))

    val results2 = client.deleteAcls(List(filterA, filterB, filterC).asJava, new DeleteAclsOptions())
    assertEquals(Set(filterA, filterB, filterC), results2.values.keySet.asScala)
    assertEquals(Set(groupAcl), results2.values.get(filterA).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(transactionalIdAcl), results2.values.get(filterC).get.values.asScala.map(_.binding).toSet)
    assertEquals(Set(acl2), results2.values.get(filterB).get.values.asScala.map(_.binding).toSet)

    waitForDescribeAcls(client, filterB, Set())
    waitForDescribeAcls(client, filterC, Set())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclDescribe(quorum: String): Unit = {
    client = createAdminClient
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.ANY), AccessControlEntryFilter.ANY)
    val allLiteralTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val allPrefixedTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED), AccessControlEntryFilter.ANY)
    val literalMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val prefixedMyTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.PREFIXED), AccessControlEntryFilter.ANY)
    val allMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.MATCH), AccessControlEntryFilter.ANY)
    val allFooTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foobar", PatternType.MATCH), AccessControlEntryFilter.ANY)

    assertEquals(Set(anyAcl), getAcls(anyAcl.toFilter))
    assertEquals(Set(prefixAcl), getAcls(prefixAcl.toFilter))
    assertEquals(Set(acl2), getAcls(acl2.toFilter))
    assertEquals(Set(fooAcl), getAcls(fooAcl.toFilter))

    assertEquals(Set(acl2), getAcls(literalMyTopic2Acls))
    assertEquals(Set(prefixAcl), getAcls(prefixedMyTopicAcls))
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allLiteralTopicAcls))
    assertEquals(Set(prefixAcl), getAcls(allPrefixedTopicAcls))
    assertEquals(Set(anyAcl, acl2, prefixAcl), getAcls(allMyTopic2Acls))
    assertEquals(Set(anyAcl, fooAcl), getAcls(allFooTopicAcls))
    assertEquals(Set(anyAcl, acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclDelete(quorum: String): Unit = {
    client = createAdminClient
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH), AccessControlEntryFilter.ANY)
    val allLiteralTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val allPrefixedTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.PREFIXED), AccessControlEntryFilter.ANY)

    // Delete only ACLs on literal 'mytopic2' topic
    var deleted = client.deleteAcls(List(acl2.toFilter).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(acl2.entry(), b.dataPlaneRequestProcessor.authorizer.get, acl2.pattern())
    }
    assertEquals(Set(anyAcl, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only ACLs on literal '*' topic
    deleted = client.deleteAcls(List(anyAcl.toFilter).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(anyAcl.entry(), b.dataPlaneRequestProcessor.authorizer.get, anyAcl.pattern())
    }
    assertEquals(Set(acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only ACLs on specific prefixed 'mytopic' topics:
    deleted = client.deleteAcls(List(prefixAcl.toFilter).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(prefixAcl.entry(), b.dataPlaneRequestProcessor.authorizer.get, prefixAcl.pattern())
    }
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all literal ACLs:
    deleted = client.deleteAcls(List(allLiteralTopicAcls).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      Set(anyAcl, acl2, fooAcl).foreach(acl =>
        TestUtils.waitAndVerifyRemovedAcl(acl.entry(), b.dataPlaneRequestProcessor.authorizer.get, acl.pattern())
      )
    }
    assertEquals(Set(prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all prefixed ACLs:
    deleted = client.deleteAcls(List(allPrefixedTopicAcls).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(prefixAcl.entry(), b.dataPlaneRequestProcessor.authorizer.get, prefixAcl.pattern())
    }
    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all topic ACLs:
    deleted = client.deleteAcls(List(allTopicAcls).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      Set(anyAcl, acl2, fooAcl, prefixAcl).foreach(acl =>
        TestUtils.waitAndVerifyRemovedAcl(acl.entry(), b.dataPlaneRequestProcessor.authorizer.get, acl.pattern())
      )
    }
    assertEquals(Set(), getAcls(allTopicAcls))
  }

  //noinspection ScalaDeprecation - test explicitly covers clients using legacy / deprecated constructors
  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testLegacyAclOpsNeverAffectOrReturnPrefixed(quorum: String): Unit = {
    client = createAdminClient
    ensureAcls(Set(anyAcl, acl2, fooAcl, prefixAcl))  // <-- prefixed exists, but should never be returned.

    val allTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.MATCH), AccessControlEntryFilter.ANY)
    val legacyAllTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyMyTopic2Acls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "mytopic2", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyAnyTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "*", PatternType.LITERAL), AccessControlEntryFilter.ANY)
    val legacyFooTopicAcls = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "foobar", PatternType.LITERAL), AccessControlEntryFilter.ANY)

    assertEquals(Set(anyAcl, acl2, fooAcl), getAcls(legacyAllTopicAcls))
    assertEquals(Set(acl2), getAcls(legacyMyTopic2Acls))
    assertEquals(Set(anyAcl), getAcls(legacyAnyTopicAcls))
    assertEquals(Set(fooAcl), getAcls(legacyFooTopicAcls))

    // Delete only (legacy) ACLs on 'mytopic2' topic
    var deleted = client.deleteAcls(List(legacyMyTopic2Acls).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(acl2.entry(), b.dataPlaneRequestProcessor.authorizer.get, acl2.pattern())
    }
    assertEquals(Set(anyAcl, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete only (legacy) ACLs on '*' topic
    deleted = client.deleteAcls(List(legacyAnyTopicAcls).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(anyAcl.entry(), b.dataPlaneRequestProcessor.authorizer.get, anyAcl.pattern())
    }
    assertEquals(Set(acl2, fooAcl, prefixAcl), getAcls(allTopicAcls))

    ensureAcls(deleted)

    // Delete all (legacy) topic ACLs:
    deleted = client.deleteAcls(List(legacyAllTopicAcls).asJava).all().get().asScala.toSet
    brokers.foreach { b =>
      Set(anyAcl, acl2, fooAcl).foreach(acl =>
        TestUtils.waitAndVerifyRemovedAcl(acl.entry(), b.dataPlaneRequestProcessor.authorizer.get, acl.pattern())
      )
    }
    assertEquals(Set(), getAcls(legacyAllTopicAcls))
    assertEquals(Set(prefixAcl), getAcls(allTopicAcls))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAttemptToCreateInvalidAcls(quorum: String): Unit = {
    client = createAdminClient
    val clusterAcl = new AclBinding(new ResourcePattern(ResourceType.CLUSTER, "foobar", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val emptyResourceNameAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.READ, AclPermissionType.ALLOW))
    val results = client.createAcls(List(clusterAcl, emptyResourceNameAcl).asJava, new CreateAclsOptions())
    assertEquals(Set(clusterAcl, emptyResourceNameAcl), results.values.keySet().asScala)
    assertFutureExceptionTypeEquals(results.values.get(clusterAcl), classOf[InvalidRequestException])
    assertFutureExceptionTypeEquals(results.values.get(emptyResourceNameAcl), classOf[InvalidRequestException])
  }

  private def verifyCauseIsClusterAuth(e: Throwable): Unit = assertEquals(classOf[ClusterAuthorizationException], e.getCause.getClass)

  private def testAclCreateGetDelete(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val result = client.createAcls(List(fooAcl, transactionalIdAcl).asJava, new CreateAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for createAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set(fooAcl))
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set(transactionalIdAcl))
    }
    TestUtils.waitUntilTrue(() => {
      val result = client.deleteAcls(List(fooAcl.toFilter, transactionalIdAcl.toFilter).asJava, new DeleteAclsOptions)
      if (expectAuth) {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(_) => true
        }
      } else {
        Try(result.all.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) =>
            assertEquals(Set(fooAcl, transactionalIdAcl), result.values.keySet)
            assertEquals(Set(fooAcl), result.values.get(fooAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            assertEquals(Set(transactionalIdAcl),
              result.values.get(transactionalIdAcl.toFilter).get.values.asScala.map(_.binding).toSet)
            true
        }
      }
    }, "timed out waiting for deleteAcls to " + (if (expectAuth) "succeed" else "fail"))
    if (expectAuth) {
      waitForDescribeAcls(client, fooAcl.toFilter, Set.empty)
      waitForDescribeAcls(client, transactionalIdAcl.toFilter, Set.empty)
    }
  }

  private def testAclGet(expectAuth: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val userAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
        new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW))
      val results = client.describeAcls(userAcl.toFilter)
      if (expectAuth) {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            false
          case Success(acls) => Set(userAcl).equals(acls.asScala.toSet)
        }
      } else {
        Try(results.values.get) match {
          case Failure(e) =>
            verifyCauseIsClusterAuth(e)
            true
          case Success(_) => false
        }
      }
    }, "timed out waiting for describeAcls to " + (if (expectAuth) "succeed" else "fail"))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclAuthorizationDenied(quorum: String): Unit = {
    client = createAdminClient

    // Test that we cannot create or delete ACLs when ALTER is denied.
    addClusterAcl(DENY, ALTER)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we cannot do anything with ACLs when DESCRIBE and ALTER are denied.
    addClusterAcl(DENY, DESCRIBE)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can create, delete, and get ACLs with the default ACLs.
    removeClusterAcl(DENY, DESCRIBE)
    removeClusterAcl(DENY, ALTER)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = true)

    // Test that we can't do anything with ACLs without the ALLOW ALTER ACL in place.
    removeClusterAcl(ALLOW, ALTER)
    removeClusterAcl(ALLOW, DELETE)
    testAclGet(expectAuth = false)
    testAclCreateGetDelete(expectAuth = false)

    // Test that we can describe, but not alter ACLs, with only the ALLOW DESCRIBE ACL in place.
    addClusterAcl(ALLOW, DESCRIBE)
    testAclGet(expectAuth = true)
    testAclCreateGetDelete(expectAuth = false)
  }

  private def addClusterAcl(permissionType: AclPermissionType, operation: AclOperation): Unit = {
    val ace = clusterAcl(permissionType, operation)
    superUserAdmin.createAcls(List(new AclBinding(clusterResourcePattern, ace)).asJava)
    brokers.foreach { b =>
      TestUtils.waitAndVerifyAcl(ace, b.dataPlaneRequestProcessor.authorizer.get, clusterResourcePattern)
    }
  }

  private def removeClusterAcl(permissionType: AclPermissionType, operation: AclOperation): Unit = {
    val ace = clusterAcl(permissionType, operation)
    superUserAdmin.deleteAcls(List(new AclBinding(clusterResourcePattern, ace).toFilter).asJava).values

    brokers.foreach { b =>
      TestUtils.waitAndVerifyRemovedAcl(ace, b.dataPlaneRequestProcessor.authorizer.get, clusterResourcePattern)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateTopicsResponseMetadataAndConfig(quorum: String): Unit = {
    val topic1 = "mytopic1"
    val topic2 = "mytopic2"
    val denyAcl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic2, PatternType.LITERAL),
      new AccessControlEntry("User:*", "*", AclOperation.DESCRIBE_CONFIGS, AclPermissionType.DENY))

    client = createAdminClient
    client.createAcls(List(denyAcl).asJava, new CreateAclsOptions()).all().get()

    val topics = Seq(topic1, topic2)
    val configsOverride = Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "100000").asJava
    val newTopics = Seq(
      new NewTopic(topic1, 2, 3.toShort).configs(configsOverride),
      new NewTopic(topic2, Option.empty[Integer].asJava, Option.empty[java.lang.Short].asJava).configs(configsOverride))
    val validateResult = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true))
    validateResult.all.get()
    waitForTopics(client, List(), topics)

    def validateMetadataAndConfigs(result: CreateTopicsResult): Unit = {
      assertEquals(2, result.numPartitions(topic1).get())
      assertEquals(3, result.replicationFactor(topic1).get())
      val topicConfigs = result.config(topic1).get().entries.asScala
      assertTrue(topicConfigs.nonEmpty)
      val segmentBytesConfig = topicConfigs.find(_.name == TopicConfig.SEGMENT_BYTES_CONFIG).get
      assertEquals(100000, segmentBytesConfig.value.toLong)
      assertEquals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, segmentBytesConfig.source)
      val compressionConfig = topicConfigs.find(_.name == TopicConfig.COMPRESSION_TYPE_CONFIG).get
      assertEquals(LogConfig.DEFAULT_COMPRESSION_TYPE, compressionConfig.value)
      assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, compressionConfig.source)

      assertFutureExceptionTypeEquals(result.numPartitions(topic2), classOf[TopicAuthorizationException])
      assertFutureExceptionTypeEquals(result.replicationFactor(topic2), classOf[TopicAuthorizationException])
      assertFutureExceptionTypeEquals(result.config(topic2), classOf[TopicAuthorizationException])
    }
    validateMetadataAndConfigs(validateResult)

    val createResult = client.createTopics(newTopics.asJava, new CreateTopicsOptions())
    createResult.all.get()
    waitForTopics(client, topics, List())
    validateMetadataAndConfigs(createResult)
    val topicIds = getTopicIds()
    assertNotEquals(Uuid.ZERO_UUID, createResult.topicId(topic1).get())
    assertEquals(topicIds(topic1), createResult.topicId(topic1).get())
    assertFutureExceptionTypeEquals(createResult.topicId(topic2), classOf[TopicAuthorizationException])
    
    val createResponseConfig = createResult.config(topic1).get().entries.asScala

    val describeResponseConfig = describeConfigs(topic1)
    assertEquals(describeResponseConfig.map(_.name).toSet, createResponseConfig.map(_.name).toSet)
    describeResponseConfig.foreach { describeEntry =>
      val name = describeEntry.name
      val createEntry = createResponseConfig.find(_.name == name).get
      assertEquals(describeEntry.value, createEntry.value, s"Value mismatch for $name")
      assertEquals(describeEntry.isReadOnly, createEntry.isReadOnly, s"isReadOnly mismatch for $name")
      assertEquals(describeEntry.isSensitive, createEntry.isSensitive, s"isSensitive mismatch for $name")
      assertEquals(describeEntry.source, createEntry.source, s"Source mismatch for $name")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testExpireDelegationToken(quorum: String): Unit = {
    client = createAdminClient
    val createDelegationTokenOptions = new CreateDelegationTokenOptions()

    // Test expiration for non-exists token
    TestUtils.assertFutureExceptionTypeEquals(
      client.expireDelegationToken("".getBytes()).expiryTimestamp(),
      classOf[DelegationTokenNotFoundException]
    )

    // Test expiring the token immediately
    val token1 = client.createDelegationToken(createDelegationTokenOptions).delegationToken().get()
    TestUtils.retry(maxWaitMs = 1000) { assertTrue(expireTokenOrFailWithAssert(token1, -1) < System.currentTimeMillis()) }

    // Test expiring the expired token
    val token2 = client.createDelegationToken(createDelegationTokenOptions.maxlifeTimeMs(1000)).delegationToken().get()
    // Ensure current time > maxLifeTimeMs of token
    Thread.sleep(1000)
    TestUtils.assertFutureExceptionTypeEquals(
      client.expireDelegationToken(token2.hmac(), new ExpireDelegationTokenOptions().expiryTimePeriodMs(1)).expiryTimestamp(),
      classOf[DelegationTokenExpiredException]
    )

    // Ensure expiring the expired token with negative expiryTimePeriodMs will not throw exception
    assertDoesNotThrow(() => expireTokenOrFailWithAssert(token2, -1))

    // Test shortening the expiryTimestamp
    val token3 = client.createDelegationToken(createDelegationTokenOptions).delegationToken().get()
    TestUtils.retry(1000) { assertTrue(expireTokenOrFailWithAssert(token3, 200) < token3.tokenInfo().expiryTimestamp()) }
  }

  private def expireTokenOrFailWithAssert(token: DelegationToken, expiryTimePeriodMs: Long): Long  = {
    try {
      client.expireDelegationToken(token.hmac(), new ExpireDelegationTokenOptions().expiryTimePeriodMs(expiryTimePeriodMs))
        .expiryTimestamp().get()
    } catch {
      // If metadata is not synced yet, the response will contain an errorCode, causing an exception to be thrown.
      // This wrapper is designed to work with TestUtils.retry
      case _: ExecutionException => throw new AssertionError("Metadata not sync yet.")
    }
  }

  private def describeConfigs(topic: String): Iterable[ConfigEntry] = {
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    var configEntries: Iterable[ConfigEntry] = null

    TestUtils.waitUntilTrue(() => {
      try {
        val topicResponse = client.describeConfigs(List(topicResource).asJava).all.get.get(topicResource)
        configEntries = topicResponse.entries.asScala
        true
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
      }
    }, "Timed out waiting for describeConfigs")

    configEntries
  }

  private def waitForDescribeAcls(client: Admin, filter: AclBindingFilter, acls: Set[AclBinding]): Unit = {
    var lastResults: util.Collection[AclBinding] = null
    TestUtils.waitUntilTrue(() => {
      lastResults = client.describeAcls(filter).values.get()
      acls == lastResults.asScala.toSet
    }, s"timed out waiting for ACLs $acls.\nActual $lastResults")
  }

  private def ensureAcls(bindings: Set[AclBinding]): Unit = {
    client.createAcls(bindings.asJava).all().get()

    bindings.foreach(binding => waitForDescribeAcls(client, binding.toFilter, Set(binding)))
  }

  private def getAcls(allTopicAcls: AclBindingFilter) = {
    client.describeAcls(allTopicAcls).values.get().asScala.toSet
  }
}
