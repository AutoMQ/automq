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
import kafka.security.authorizer.AclEntry.WildcardHost
import kafka.server.{BaseRequestTest, KafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION
import org.apache.kafka.common.acl.AclPermissionType.ALLOW
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.{Resource, ResourcePattern}
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, GROUP, TOPIC, TRANSACTIONAL_ID}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import java.util.Properties

object AbstractAuthorizerIntegrationTest {
  val BrokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "broker")
  val ClientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "client")

  val BrokerListenerName = "BROKER"
  val ClientListenerName = "CLIENT"
  val ControllerListenerName = "CONTROLLER"

  class PrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context.listenerName match {
        case BrokerListenerName | ControllerListenerName => BrokerPrincipal
        case ClientListenerName => ClientPrincipal
        case listenerName => throw new IllegalArgumentException(s"No principal mapped to listener $listenerName")
      }
    }
  }
}

/**
 * Abstract authorizer test to be used both in scala and java tests of authorizer.
 */
class AbstractAuthorizerIntegrationTest extends BaseRequestTest {
  import AbstractAuthorizerIntegrationTest._

  override def interBrokerListenerName: ListenerName = new ListenerName(BrokerListenerName)
  override def listenerName: ListenerName = new ListenerName(ClientListenerName)
  override def brokerCount: Int = 1

  def clientPrincipal: KafkaPrincipal = ClientPrincipal
  def brokerPrincipal: KafkaPrincipal = BrokerPrincipal

  val clientPrincipalString: String = clientPrincipal.toString

  val brokerId: Integer = 0
  val topic = "topic"
  val topicPattern = "topic.*"
  val transactionalId = "transactional.id"
  val producerId = 83392L
  val part = 0
  val correlationId = 0
  val clientId = "client-Id"
  val tp = new TopicPartition(topic, part)
  val logDir = "logDir"
  val group = "my-group"
  val protocolType = "consumer"
  val protocolName = "consumer-range"
  val clusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL)
  val topicResource = new ResourcePattern(TOPIC, topic, LITERAL)
  val groupResource = new ResourcePattern(GROUP, group, LITERAL)
  val transactionalIdResource = new ResourcePattern(TRANSACTIONAL_ID, transactionalId, LITERAL)

  producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "1")
  producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
  producerConfig.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "50000")
  consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    addNodeProperties(properties)
  }

  override def kraftControllerConfigs(): collection.Seq[Properties] = {
    val controllerConfigs = super.kraftControllerConfigs()
    controllerConfigs.foreach(addNodeProperties)
    controllerConfigs
  }

  private def addNodeProperties(properties: Properties): Unit = {
    if (isKRaftTest()) {
      properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[StandardAuthorizer].getName)
      properties.put(StandardAuthorizer.SUPER_USERS_CONFIG, BrokerPrincipal.toString)
    } else {
      properties.put(KafkaConfig.AuthorizerClassNameProp, classOf[AclAuthorizer].getName)
    }

    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
    properties.put(KafkaConfig.UnstableApiVersionsEnableProp, "true")
    properties.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, classOf[PrincipalBuilder].getName)
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    doSetup(testInfo, createOffsetsTopic = false)

    // Allow inter-broker communication
    addAndVerifyAcls(Set(new AccessControlEntry(brokerPrincipal.toString, WildcardHost, CLUSTER_ACTION, ALLOW)), clusterResource)

    createOffsetsTopic(listenerName = interBrokerListenerName)
  }
}
