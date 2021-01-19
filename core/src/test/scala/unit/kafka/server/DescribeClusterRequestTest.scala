/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.lang.{Byte => JByte}
import java.util.Properties
import kafka.network.SocketServer
import kafka.security.authorizer.AclEntry
import org.apache.kafka.common.message.{DescribeClusterRequestData, DescribeClusterResponseData}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{DescribeClusterRequest, DescribeClusterResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._

class DescribeClusterRequestTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.DefaultReplicationFactorProp, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)
  }

  @Test
  def testDescribeClusterRequestIncludingClusterAuthorizedOperations(): Unit = {
    testDescribeClusterRequest(true)
  }

  @Test
  def testDescribeClusterRequestExcludingClusterAuthorizedOperations(): Unit = {
    testDescribeClusterRequest(false)
  }

  def testDescribeClusterRequest(includeClusterAuthorizedOperations: Boolean): Unit = {
    val expectedBrokers = servers.map { server =>
      new DescribeClusterResponseData.DescribeClusterBroker()
        .setBrokerId(server.config.brokerId)
        .setHost("localhost")
        .setPort(server.socketServer.boundPort(listenerName))
        .setRack(server.config.rack.orNull)
    }.toSet
    val expectedControllerId = servers.filter(_.kafkaController.isActive).last.config.brokerId
    val expectedClusterId = servers.last.clusterId

    val expectedClusterAuthorizedOperations = if (includeClusterAuthorizedOperations) {
      Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.CLUSTER)
          .map(_.code.asInstanceOf[JByte]).asJava)
    } else {
      Int.MinValue
    }

    for (version <- ApiKeys.DESCRIBE_CLUSTER.oldestVersion to ApiKeys.DESCRIBE_CLUSTER.latestVersion) {
      val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
        .setIncludeClusterAuthorizedOperations(includeClusterAuthorizedOperations))
        .build(version.toShort)
      val describeClusterResponse = sentDescribeClusterRequest(describeClusterRequest)

      assertEquals(expectedControllerId, describeClusterResponse.data.controllerId)
      assertEquals(expectedClusterId, describeClusterResponse.data.clusterId)
      assertEquals(expectedClusterAuthorizedOperations, describeClusterResponse.data.clusterAuthorizedOperations)
      assertEquals(expectedBrokers, describeClusterResponse.data.brokers.asScala.toSet)
    }
  }

  private def sentDescribeClusterRequest(request: DescribeClusterRequest, destination: Option[SocketServer] = None): DescribeClusterResponse = {
    connectAndReceive[DescribeClusterResponse](request, destination = destination.getOrElse(anySocketServer))
  }
}
