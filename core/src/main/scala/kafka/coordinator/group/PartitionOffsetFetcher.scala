package kafka.coordinator.group

import kafka.server.MetadataCache
import kafka.utils.Logging
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.AutomqGetOffsetTimestampsRequestData
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.requests.s3.{AutomqGetOffsetTimestampsRequest, AutomqGetOffsetTimestampsResponse}
import org.apache.kafka.server.util.AsyncSender

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Fetches log end offsets from remote brokers using AsyncSender.
 */
case class PartitionOffsets(
  topic: String,
  partition: Int,
  logEndOffset: Long,
  logStartOffset: Long,
  logEndTimestamp: Long,
  logStartTimestamp: Long
)

class PartitionOffsetFetcher(
  brokerId: Int,
  metadataCache: MetadataCache,
  listenerName: ListenerName,
  asyncSender: AsyncSender
) extends Logging {

  /**
   * Fetch LEOs for the given partitions from their leaders.
   * Returns a map of partition -> LEO for successful fetches.
   * Failed fetches are omitted from the result.
   */
  def fetchLeos(partitions: Set[TopicPartition]): CompletableFuture[Map[TopicPartition, Long]] = {
    if (partitions.isEmpty) {
      return CompletableFuture.completedFuture(Map.empty)
    }
    fetchOffsetsWithTimestamp(partitions.toSeq, ListOffsetsRequest.LATEST_TIMESTAMP)
      .thenApply[Map[TopicPartition, Long]] { results =>
        results.map { case (tp, (offset, _)) => tp -> offset }
      }
  }

  /**
   * Fetch comprehensive offset information for all partitions of a topic.
   * Returns a list of PartitionOffsets containing start/end offsets and timestamps.
   * 
   * Note: Uses LATEST_TIMESTAMP instead of MAX_TIMESTAMP to avoid reading message data.
   * In most cases the latest offset has the maximum timestamp, but out-of-order messages
   * may cause timestamp inaccuracy.
   */
  def fetchLogEndOffsets(topic: String): CompletableFuture[java.util.List[PartitionOffsets]] = {
    val partitions = metadataCache.getTopicPartitions(topic)
    if (partitions.isEmpty) {
      return CompletableFuture.completedFuture(new java.util.ArrayList[PartitionOffsets]())
    }

    val tps = partitions.toSeq
    val earliestFuture = fetchOffsetsWithTimestamp(tps, ListOffsetsRequest.EARLIEST_TIMESTAMP)
    val latestFuture = fetchOffsetsWithTimestamp(tps, ListOffsetsRequest.LATEST_TIMESTAMP)

    CompletableFuture.allOf(earliestFuture, latestFuture)
      .thenApply[java.util.List[PartitionOffsets]] { _ =>
        val earliestResults = earliestFuture.join()
        val latestResults = latestFuture.join()

        val results = new java.util.ArrayList[PartitionOffsets]()
        tps.zipWithIndex.foreach { case (tp, idx) =>
          val earliest = earliestResults.getOrElse(tp, (0L, 0L))
          val latest = latestResults.getOrElse(tp, (0L, -1L))

          val logStartOffset = if (earliest._1 < 0) 0L else earliest._1
          val logEndOffset = latest._1
          val logEndTimestamp = latest._2

          if (idx == 0) {
            info(s"Partition $tp: logStartOffset=$logStartOffset, logEndOffset=$logEndOffset, " +
              s"logStartTimestamp=${earliest._2}, logEndTimestamp=$logEndTimestamp")
          } else {
            debug(s"Partition $tp: logStartOffset=$logStartOffset, logEndOffset=$logEndOffset, " +
              s"logStartTimestamp=${earliest._2}, logEndTimestamp=$logEndTimestamp")
          }

          results.add(PartitionOffsets(topic, tp.partition, logEndOffset, logStartOffset,
            logEndTimestamp, earliest._2))
        }
        results
      }
  }

  /**
   * Fetch timestamps for given (partition, offset) pairs from their leaders.
   * Returns a map of (TopicPartition, offset) -> timestamp.
   */
  def fetchOffsetTimestamps(
    queries: Map[TopicPartition, Set[Long]]
  ): CompletableFuture[Map[(TopicPartition, Long), Long]] = {
    if (queries.isEmpty) {
      return CompletableFuture.completedFuture(Map.empty)
    }

    val partitionsByLeader = new java.util.HashMap[Node, java.util.List[(TopicPartition, Set[Long])]]()
    queries.foreach { case (tp, offsets) =>
      metadataCache.getPartitionLeaderEndpoint(tp.topic, tp.partition, listenerName).foreach { node =>
        partitionsByLeader
          .computeIfAbsent(node, _ => new java.util.ArrayList[(TopicPartition, Set[Long])]())
          .add((tp, offsets))
      }
    }

    if (partitionsByLeader.isEmpty) {
      return CompletableFuture.completedFuture(Map.empty)
    }

    val futures = new java.util.ArrayList[CompletableFuture[Map[(TopicPartition, Long), Long]]]()
    partitionsByLeader.forEach { (node, nodeQueries) =>
      val builder = buildGetOffsetTimestampsRequest(nodeQueries.asScala.toSeq)
      val future = asyncSender.sendRequest(node, builder)
        .thenApply[Map[(TopicPartition, Long), Long]] { response =>
          parseGetOffsetTimestampsResponse(
            response.responseBody.asInstanceOf[AutomqGetOffsetTimestampsResponse]
          )
        }
        .exceptionally { _ => Map.empty[(TopicPartition, Long), Long] }
      futures.add(future)
    }

    CompletableFuture.allOf(futures.toArray(new Array[CompletableFuture[_]](0)): _*)
      .thenApply[Map[(TopicPartition, Long), Long]] { _ =>
        futures.asScala.flatMap(_.join()).toMap
      }
  }

  private def fetchOffsetsWithTimestamp(partitions: Seq[TopicPartition], timestamp: Long):
      CompletableFuture[Map[TopicPartition, (Long, Long)]] = {
    val partitionsByLeader = new java.util.HashMap[Node, java.util.List[TopicPartition]]()
    partitions.foreach { tp =>
      metadataCache.getPartitionLeaderEndpoint(tp.topic, tp.partition, listenerName).foreach { node =>
        partitionsByLeader.computeIfAbsent(node, _ => new java.util.ArrayList[TopicPartition]()).add(tp)
      }
    }

    if (partitionsByLeader.isEmpty) {
      return CompletableFuture.completedFuture(Map.empty)
    }

    val futures = new java.util.ArrayList[CompletableFuture[Map[TopicPartition, (Long, Long)]]]()
    partitionsByLeader.forEach { (node, nodePartitions) =>
      val builder = buildListOffsetsRequestWithTimestamp(nodePartitions.asScala.toSeq, timestamp)
      val future = asyncSender.sendRequest(node, builder)
        .thenApply[Map[TopicPartition, (Long, Long)]] { response =>
          parseListOffsetsResponseWithTimestamp(response.responseBody.asInstanceOf[ListOffsetsResponse])
        }
        .exceptionally { _ => Map.empty[TopicPartition, (Long, Long)] }
      futures.add(future)
    }

    CompletableFuture.allOf(futures.toArray(new Array[CompletableFuture[_]](0)): _*)
      .thenApply[Map[TopicPartition, (Long, Long)]] { _ =>
        futures.asScala.flatMap(_.join()).toMap
      }
  }

  private def buildListOffsetsRequestWithTimestamp(partitions: Seq[TopicPartition], timestamp: Long):
      ListOffsetsRequest.Builder = {
    val topics = new java.util.ArrayList[ListOffsetsTopic]()
    partitions.groupBy(_.topic).foreach { case (topic, tps) =>
      val topicData = new ListOffsetsTopic().setName(topic)
      tps.foreach { tp =>
        topicData.partitions.add(
          new ListOffsetsPartition()
            .setPartitionIndex(tp.partition)
            .setTimestamp(timestamp)
        )
      }
      topics.add(topicData)
    }
    ListOffsetsRequest.Builder.forReplica(
      org.apache.kafka.server.common.MetadataVersion.latestTesting().listOffsetRequestVersion,
      brokerId
    ).setTargetTimes(topics)
  }

  private def parseListOffsetsResponseWithTimestamp(response: ListOffsetsResponse):
      Map[TopicPartition, (Long, Long)] = {
    val result = scala.collection.mutable.Map[TopicPartition, (Long, Long)]()
    response.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        if (Errors.forCode(partition.errorCode) == Errors.NONE) {
          result.put(new TopicPartition(topic.name, partition.partitionIndex),
            (partition.offset, partition.timestamp))
        }
      }
    }
    result.toMap
  }

  private def buildGetOffsetTimestampsRequest(
    queries: Seq[(TopicPartition, Set[Long])]
  ): AutomqGetOffsetTimestampsRequest.Builder = {
    val data = new AutomqGetOffsetTimestampsRequestData()
    queries.groupBy(_._1.topic).foreach { case (topic, tpQueries) =>
      val topicReq = new AutomqGetOffsetTimestampsRequestData.TopicRequest()
        .setTopicId(metadataCache.getTopicId(topic))
      tpQueries.foreach { case (tp, offsets) =>
        val partReq = new AutomqGetOffsetTimestampsRequestData.PartitionRequest()
          .setPartitionIndex(tp.partition)
        offsets.foreach(o => partReq.offsets.add(o))
        topicReq.partitions.add(partReq)
      }
      data.topics.add(topicReq)
    }
    new AutomqGetOffsetTimestampsRequest.Builder(data)
  }

  private def parseGetOffsetTimestampsResponse(
    response: AutomqGetOffsetTimestampsResponse
  ): Map[(TopicPartition, Long), Long] = {
    val result = scala.collection.mutable.Map[(TopicPartition, Long), Long]()
    response.data.topics.forEach { topic =>
      metadataCache.getTopicName(topic.topicId).foreach { topicName =>
        topic.partitions.forEach { partition =>
          if (Errors.forCode(partition.errorCode) == Errors.NONE) {
            partition.offsetTimestamps.forEach { offsetTs =>
              if (offsetTs.timestamp >= 0) {
                result.put(
                  (new TopicPartition(topicName, partition.partitionIndex), offsetTs.offset),
                  offsetTs.timestamp
                )
              }
            }
          }
        }
      }
    }
    result.toMap
  }
}
