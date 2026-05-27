/*
 * Copyright 2026, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.retrystorm

import org.apache.kafka.common.message.FetchResponseData.{FetchableTopicResponse, PartitionData}
import org.apache.kafka.common.message.{JoinGroupRequestData, JoinGroupResponseData}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.message.ProduceResponseData.{PartitionProduceResponse, TopicProduceResponse}
import org.apache.kafka.common.message.{FetchResponseData, FindCoordinatorResponseData, ListOffsetsResponseData, MetadataResponseData, OffsetForLeaderEpochResponseData, ProduceResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchResponse, FindCoordinatorResponse, JoinGroupRequest, JoinGroupResponse, ListOffsetsResponse, MetadataResponse, OffsetsForLeaderEpochResponse, ProduceResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("S3Unit")
class RetryStormResponseSummaryExtractorsTest {

  @Test
  def testProduceExtractorClassifiesSuccessAndTransient(): Unit = {
    val data = new ProduceResponseData()
    data.responses().add(new TopicProduceResponse().setName("topic").setPartitionResponses(java.util.List.of(
      new PartitionProduceResponse().setIndex(0).setErrorCode(Errors.NONE.code()),
      new PartitionProduceResponse().setIndex(1).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
    )))

    val summary = ProduceResponseSummaryExtractor(null, new ProduceResponse(data))
    assertTrue(summary.resources.exists(_.valid))
    assertTrue(summary.resources.exists(_.delayableTransient))
  }

  @Test
  def testFetchExtractorClassifiesNoneAsValidAndTransientErrors(): Unit = {
    val data = new FetchResponseData()
    data.responses().add(new FetchableTopicResponse().setTopic("topic").setPartitions(java.util.List.of(
      new PartitionData().setPartitionIndex(0).setErrorCode(Errors.NONE.code()),
      new PartitionData().setPartitionIndex(1).setErrorCode(Errors.FENCED_LEADER_EPOCH.code())
    )))

    val summary = FetchResponseSummaryExtractor(null, new FetchResponse(data))
    assertTrue(summary.resources.exists(_.valid))
    assertTrue(summary.resources.exists(_.delayableTransient))
  }

  @Test
  def testListOffsetsExtractorRequiresValidOffset(): Unit = {
    val data = new ListOffsetsResponseData()
    data.topics().add(new ListOffsetsTopicResponse().setName("topic").setPartitions(java.util.List.of(
      new ListOffsetsPartitionResponse().setPartitionIndex(0).setErrorCode(Errors.NONE.code()).setOffset(42L),
      new ListOffsetsPartitionResponse().setPartitionIndex(1).setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).setOffset(-1L)
    )))

    val summary = ListOffsetsResponseSummaryExtractor(null, new ListOffsetsResponse(data))
    assertTrue(summary.resources.exists(_.valid))
    assertTrue(summary.resources.exists(resource => resource.protective && !resource.delayableTransient))
  }

  @Test
  def testOffsetForLeaderEpochExtractorClassifiesTransient(): Unit = {
    val topics = new OffsetForLeaderTopicResultCollection()
    topics.add(new OffsetForLeaderTopicResult().setTopic("topic").setPartitions(java.util.List.of(
      new EpochEndOffset().setPartition(0).setErrorCode(Errors.LEADER_NOT_AVAILABLE.code()).setEndOffset(-1L)
    )).duplicate())
    val data = new OffsetForLeaderEpochResponseData().setTopics(topics)

    val summary = OffsetForLeaderEpochResponseSummaryExtractor(null, new OffsetsForLeaderEpochResponse(data))
    assertTrue(summary.resources.head.delayableTransient)
  }

  @Test
  def testMetadataExtractorClassifiesLeaderUnavailable(): Unit = {
    val data = new MetadataResponseData()
    data.topics().add(new MetadataResponseTopic().setName("topic").setErrorCode(Errors.LEADER_NOT_AVAILABLE.code()).setPartitions(java.util.List.of(
      new MetadataResponsePartition().setPartitionIndex(0).setErrorCode(Errors.LEADER_NOT_AVAILABLE.code()).setLeaderId(-1)
    )))

    val summary = MetadataResponseSummaryExtractor(null, new MetadataResponse(data, 12.toShort))
    assertTrue(summary.resources.exists(_.delayableTransient))
  }

  @Test
  def testFindCoordinatorExtractorClassifiesCoordinatorLoading(): Unit = {
    val data = new FindCoordinatorResponseData()
    data.coordinators().add(new FindCoordinatorResponseData.Coordinator()
      .setKey("group")
      .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
      .setNodeId(-1))

    val summary = FindCoordinatorResponseSummaryExtractor(null, new FindCoordinatorResponse(data))
    assertTrue(summary.resources.head.delayableTransient)
  }

  @Test
  def testCoordinatorApisRegisteredButHeartbeatExcluded(): Unit = {
    assertTrue(RetryStormResponseSummaryExtractors.DefaultRegistry.contains(org.apache.kafka.common.protocol.ApiKeys.JOIN_GROUP))
    assertTrue(RetryStormResponseSummaryExtractors.DefaultRegistry.contains(org.apache.kafka.common.protocol.ApiKeys.OFFSET_COMMIT))
    assertFalse(RetryStormResponseSummaryExtractors.DefaultRegistry.contains(org.apache.kafka.common.protocol.ApiKeys.HEARTBEAT))
    assertFalse(RetryStormResponseSummaryExtractors.DefaultRegistry.contains(org.apache.kafka.common.protocol.ApiKeys.CONSUMER_GROUP_HEARTBEAT))
  }

  @Test
  def testCoordinatorExtractorUsesGroupResourceKey(): Unit = {
    val request = new JoinGroupRequest(
      new JoinGroupRequestData()
        .setGroupId("group-a")
        .setSessionTimeoutMs(10000)
        .setRebalanceTimeoutMs(10000),
      5.toShort
    )
    val version = org.apache.kafka.common.protocol.ApiKeys.JOIN_GROUP.latestVersion()
    val response = new JoinGroupResponse(
      new JoinGroupResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
      version
    )

    val summary = CoordinatorResponseSummaryExtractor(request, response)
    assertEquals("group-group-a", summary.resources.head.resourceKey)
    assertTrue(summary.resources.head.delayableTransient)
  }
}
