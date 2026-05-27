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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import kafka.network.RequestChannel

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

trait RetryStormResponseSummaryExtractor extends ((AnyRef, AnyRef) => ResponseSummary)

object RetryStormResponseSummaryExtractors {
  val DelayableLeaderErrors: Set[Errors] = Set(
    Errors.NOT_LEADER_OR_FOLLOWER,
    Errors.LEADER_NOT_AVAILABLE,
    Errors.FENCED_LEADER_EPOCH
  )

  val DelayableCoordinatorErrors: Set[Errors] = Set(
    Errors.COORDINATOR_LOAD_IN_PROGRESS,
    Errors.COORDINATOR_NOT_AVAILABLE,
    Errors.NOT_COORDINATOR
  )

  val DefaultRegistry: Map[ApiKeys, (AnyRef, AnyRef) => ResponseSummary] = Map(
    ApiKeys.PRODUCE -> ProduceResponseSummaryExtractor,
    ApiKeys.FETCH -> FetchResponseSummaryExtractor,
    ApiKeys.LIST_OFFSETS -> ListOffsetsResponseSummaryExtractor,
    ApiKeys.OFFSET_FOR_LEADER_EPOCH -> OffsetForLeaderEpochResponseSummaryExtractor,
    ApiKeys.METADATA -> MetadataResponseSummaryExtractor,
    ApiKeys.DESCRIBE_TOPIC_PARTITIONS -> DescribeTopicPartitionsResponseSummaryExtractor,
    ApiKeys.FIND_COORDINATOR -> FindCoordinatorResponseSummaryExtractor,
    ApiKeys.JOIN_GROUP -> CoordinatorResponseSummaryExtractor,
    ApiKeys.SYNC_GROUP -> CoordinatorResponseSummaryExtractor,
    ApiKeys.OFFSET_COMMIT -> CoordinatorResponseSummaryExtractor,
    ApiKeys.OFFSET_FETCH -> CoordinatorResponseSummaryExtractor,
    ApiKeys.LEAVE_GROUP -> CoordinatorResponseSummaryExtractor,
    ApiKeys.TXN_OFFSET_COMMIT -> CoordinatorResponseSummaryExtractor,
    ApiKeys.ADD_OFFSETS_TO_TXN -> CoordinatorResponseSummaryExtractor,
    ApiKeys.INIT_PRODUCER_ID -> CoordinatorResponseSummaryExtractor,
    ApiKeys.END_TXN -> CoordinatorResponseSummaryExtractor
  )

  def classify(resourceKey: String, error: Errors, delayableErrors: Set[Errors]): ResourceResult = {
    ResourceResult(
      resourceKey = resourceKey,
      valid = error == Errors.NONE,
      delayableTransient = delayableErrors.contains(error),
      protective = error != Errors.NONE
    )
  }
}

object ProduceResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val produceResponse = response.asInstanceOf[ProduceResponse]
    val resources = produceResponse.data().responses().asScala.flatMap { topic =>
      topic.partitionResponses().asScala.map { partition =>
        RetryStormResponseSummaryExtractors.classify(
          s"${topic.name()}-${partition.index()}",
          Errors.forCode(partition.errorCode()),
          RetryStormResponseSummaryExtractors.DelayableLeaderErrors
        )
      }
    }.toSeq
    ResponseSummary(resources)
  }
}

object FetchResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val fetchResponse = response.asInstanceOf[FetchResponse]
    val resources = fetchResponse.data().responses().asScala.flatMap { topic =>
      val topicKey = if (topic.topic() != null && topic.topic().nonEmpty) topic.topic() else topic.topicId().toString
      topic.partitions().asScala.map { partition =>
        val error = Errors.forCode(partition.errorCode())
        val records = partition.records()
        val hasRecords = records != null && records.isInstanceOf[Records] && records.asInstanceOf[Records].sizeInBytes() > 0
        ResourceResult(
          resourceKey = s"$topicKey-${partition.partitionIndex()}",
          valid = error == Errors.NONE || hasRecords,
          delayableTransient = RetryStormResponseSummaryExtractors.DelayableLeaderErrors.contains(error),
          protective = error != Errors.NONE
        )
      }
    }.toSeq
    ResponseSummary(resources, fetchDelayCapMs(request))
  }

  private def fetchDelayCapMs(request: AnyRef): Option[Long] = {
    request match {
      case channelRequest: RequestChannel.Request =>
        val fetchRequest = channelRequest.body[FetchRequest]
        val elapsedMs =
          if (channelRequest.requestDequeueTimeNanos < 0) 0L
          else TimeUnit.NANOSECONDS.toMillis(Time.SYSTEM.nanoseconds() - channelRequest.requestDequeueTimeNanos)
        Some(math.max(fetchRequest.maxWait().toLong - elapsedMs, 0L))
      case _ =>
        None
    }
  }
}

object ListOffsetsResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val listOffsetsResponse = response.asInstanceOf[ListOffsetsResponse]
    val resources = listOffsetsResponse.data().topics().asScala.flatMap { topic =>
      topic.partitions().asScala.map { partition =>
        val error = Errors.forCode(partition.errorCode())
        ResourceResult(
          resourceKey = s"${topic.name()}-${partition.partitionIndex()}",
          valid = error == Errors.NONE && partition.offset() != ListOffsetsResponse.UNKNOWN_OFFSET,
          delayableTransient = RetryStormResponseSummaryExtractors.DelayableLeaderErrors.contains(error),
          protective = error != Errors.NONE
        )
      }
    }.toSeq
    ResponseSummary(resources)
  }
}

object OffsetForLeaderEpochResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val leaderEpochResponse = response.asInstanceOf[OffsetsForLeaderEpochResponse]
    val resources = leaderEpochResponse.data().topics().asScala.flatMap { topic =>
      topic.partitions().asScala.map { partition =>
        val error = Errors.forCode(partition.errorCode())
        ResourceResult(
          resourceKey = s"${topic.topic()}-${partition.partition()}",
          valid = error == Errors.NONE && partition.endOffset() != OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET,
          delayableTransient = RetryStormResponseSummaryExtractors.DelayableLeaderErrors.contains(error),
          protective = error != Errors.NONE
        )
      }
    }.toSeq
    ResponseSummary(resources)
  }
}

object MetadataResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val metadataResponse = response.asInstanceOf[MetadataResponse]
    val hasClusterMetadata = metadataResponse.data().brokers().asScala.nonEmpty ||
      metadataResponse.data().controllerId() >= 0
    val resources = metadataResponse.data().topics().asScala.flatMap { topic =>
      val topicName = if (topic.name() != null) topic.name() else topic.topicId().toString
      val topicResult = RetryStormResponseSummaryExtractors.classify(
        topicName,
        Errors.forCode(topic.errorCode()),
        RetryStormResponseSummaryExtractors.DelayableLeaderErrors
      )
      val partitionResults = topic.partitions().asScala.map { partition =>
        val error = Errors.forCode(partition.errorCode())
        ResourceResult(
          resourceKey = s"$topicName-${partition.partitionIndex()}",
          valid = error == Errors.NONE && partition.leaderId() >= 0,
          delayableTransient = RetryStormResponseSummaryExtractors.DelayableLeaderErrors.contains(error),
          protective = error != Errors.NONE
        )
      }
      topicResult +: partitionResults.toSeq
    }.toSeq
    if (resources.nonEmpty) {
      ResponseSummary(ResourceResult("cluster", valid = hasClusterMetadata, delayableTransient = false, protective = false) +: resources)
    } else {
      ResponseSummary(Seq(ResourceResult("cluster", valid = hasClusterMetadata, delayableTransient = false, protective = false)))
    }
  }
}

object DescribeTopicPartitionsResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val describeResponse = response.asInstanceOf[DescribeTopicPartitionsResponse]
    val resources = describeResponse.data().topics().asScala.flatMap { topic =>
      val topicName = if (topic.name() != null) topic.name() else Uuid.ZERO_UUID.toString
      val topicError = Errors.forCode(topic.errorCode())
      val topicResult = ResourceResult(
        topicName,
        valid = topicError == Errors.NONE && topic.partitions().asScala.nonEmpty,
        delayableTransient = RetryStormResponseSummaryExtractors.DelayableLeaderErrors.contains(topicError),
        protective = topicError != Errors.NONE
      )
      val partitionResults = topic.partitions().asScala.map { partition =>
        val error = Errors.forCode(partition.errorCode())
        ResourceResult(
          s"$topicName-${partition.partitionIndex()}",
          valid = error == Errors.NONE,
          delayableTransient = RetryStormResponseSummaryExtractors.DelayableLeaderErrors.contains(error),
          protective = error != Errors.NONE
        )
      }
      topicResult +: partitionResults.toSeq
    }.toSeq
    ResponseSummary(resources)
  }
}

object FindCoordinatorResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val findCoordinatorResponse = response.asInstanceOf[FindCoordinatorResponse]
    val coordinators = findCoordinatorResponse.data().coordinators()
    val resources =
      if (!coordinators.isEmpty) {
        coordinators.asScala.map { coordinator =>
          val error = Errors.forCode(coordinator.errorCode())
          ResourceResult(
            resourceKey = s"${coordinatorType(request)}-${coordinator.key()}",
            valid = error == Errors.NONE && coordinator.nodeId() >= 0,
            delayableTransient = RetryStormResponseSummaryExtractors.DelayableCoordinatorErrors.contains(error),
            protective = error != Errors.NONE
          )
        }.toSeq
      } else {
        val error = findCoordinatorResponse.error()
        Seq(ResourceResult(
          resourceKey = coordinatorKey(request),
          valid = error == Errors.NONE && findCoordinatorResponse.data().nodeId() >= 0,
          delayableTransient = RetryStormResponseSummaryExtractors.DelayableCoordinatorErrors.contains(error),
          protective = error != Errors.NONE
        ))
      }
    ResponseSummary(resources)
  }

  private def coordinatorKey(request: AnyRef): String = {
    val body = request match {
      case channelRequest: RequestChannel.Request => channelRequest.body[FindCoordinatorRequest]
      case findCoordinatorRequest: FindCoordinatorRequest => findCoordinatorRequest
      case _ => null
    }
    if (body == null) {
      "coordinator"
    } else {
      val key = Option(body.data().key()).filter(_.nonEmpty)
        .orElse(body.data().coordinatorKeys().asScala.headOption)
        .getOrElse("")
      s"${coordinatorType(body)}-$key"
    }
  }

  private def coordinatorType(request: AnyRef): String = {
    val body = request match {
      case channelRequest: RequestChannel.Request => channelRequest.body[FindCoordinatorRequest]
      case findCoordinatorRequest: FindCoordinatorRequest => findCoordinatorRequest
      case _ => null
    }
    if (body == null) "coordinator"
    else FindCoordinatorRequest.CoordinatorType.forId(body.data().keyType()).name().toLowerCase
  }
}

object CoordinatorResponseSummaryExtractor extends RetryStormResponseSummaryExtractor {
  override def apply(request: AnyRef, response: AnyRef): ResponseSummary = {
    val abstractResponse = response.asInstanceOf[AbstractResponse]
    val errors = abstractResponse.errorCounts().keySet().asScala.toSeq
    val hasValid = errors.contains(Errors.NONE)
    val nonSuccessErrors = errors.filter(_ != Errors.NONE)
    val allCoordinatorTransient = nonSuccessErrors.nonEmpty &&
      nonSuccessErrors.forall(RetryStormResponseSummaryExtractors.DelayableCoordinatorErrors.contains)
    ResponseSummary(Seq(ResourceResult(
      resourceKey = coordinatorResourceKey(request),
      valid = hasValid,
      delayableTransient = allCoordinatorTransient,
      protective = nonSuccessErrors.nonEmpty
    )))
  }

  private def coordinatorResourceKey(request: AnyRef): String = {
    val body = request match {
      case channelRequest: RequestChannel.Request => channelRequest.body[AbstractRequest]
      case abstractRequest: AbstractRequest => abstractRequest
      case _ => null
    }

    body match {
      case joinGroup: JoinGroupRequest => groupKey(joinGroup.data().groupId())
      case syncGroup: SyncGroupRequest => groupKey(syncGroup.data().groupId())
      case offsetCommit: OffsetCommitRequest => groupKey(offsetCommit.data().groupId())
      case offsetFetch: OffsetFetchRequest => groupKey(offsetFetch.groupId())
      case leaveGroup: LeaveGroupRequest => groupKey(leaveGroup.data().groupId())
      case txnOffsetCommit: TxnOffsetCommitRequest => transactionKey(txnOffsetCommit.data().transactionalId())
      case addOffsetsToTxn: AddOffsetsToTxnRequest => transactionKey(addOffsetsToTxn.data().transactionalId())
      case initProducerId: InitProducerIdRequest =>
        Option(initProducerId.data().transactionalId()).filter(_.nonEmpty)
          .map(transactionKey)
          .getOrElse(s"producer-${initProducerId.data().producerId()}")
      case endTxn: EndTxnRequest =>
        Option(endTxn.data().transactionalId()).filter(_.nonEmpty)
          .map(transactionKey)
          .getOrElse(s"producer-${endTxn.data().producerId()}")
      case _ => "coordinator"
    }
  }

  private def groupKey(groupId: String): String = s"group-${Option(groupId).getOrElse("")}"

  private def transactionKey(transactionalId: String): String = s"transaction-${Option(transactionalId).getOrElse("")}"
}
