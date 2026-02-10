package kafka.coordinator.group

import kafka.server.MetadataCache
import kafka.utils.Logging
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.server.util.AsyncSender

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Fetches log end offsets from remote brokers using AsyncSender.
 */
class RemoteLeoFetcher(
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

    // Group partitions by leader
    val partitionsByLeader = new java.util.HashMap[Node, java.util.List[TopicPartition]]()
    partitions.foreach { tp =>
      metadataCache.getPartitionLeaderEndpoint(tp.topic, tp.partition, listenerName) match {
        case Some(node) =>
          partitionsByLeader.computeIfAbsent(node, _ => new java.util.ArrayList[TopicPartition]()).add(tp)
        case None =>
          debug(s"No leader found for $tp, skipping remote LEO fetch")
      }
    }

    if (partitionsByLeader.isEmpty) {
      return CompletableFuture.completedFuture(Map.empty)
    }

    // Send requests to each leader and collect results
    val futures = new java.util.ArrayList[CompletableFuture[Map[TopicPartition, Long]]]()
    
    partitionsByLeader.forEach { (node, nodePartitions) =>
      val builder = buildListOffsetsRequest(nodePartitions.asScala.toSeq)
      val future = asyncSender.sendRequest(node, builder)
        .thenApply[Map[TopicPartition, Long]] { response =>
          parseListOffsetsResponse(response.responseBody.asInstanceOf[ListOffsetsResponse])
        }
        .exceptionally { ex =>
          debug(s"Failed to fetch LEO from $node: ${ex.getMessage}")
          Map.empty[TopicPartition, Long]
        }
      futures.add(future)
    }

    // Combine all results
    CompletableFuture.allOf(futures.toArray(new Array[CompletableFuture[_]](0)): _*)
      .thenApply[Map[TopicPartition, Long]] { _ =>
        futures.asScala.flatMap(_.join()).toMap
      }
  }

  private def buildListOffsetsRequest(partitions: Seq[TopicPartition]): ListOffsetsRequest.Builder = {
    val topics = new java.util.ArrayList[ListOffsetsTopic]()
    partitions.groupBy(_.topic).foreach { case (topic, tps) =>
      val topicData = new ListOffsetsTopic().setName(topic)
      tps.foreach { tp =>
        topicData.partitions.add(
          new ListOffsetsPartition()
            .setPartitionIndex(tp.partition)
            .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
        )
      }
      topics.add(topicData)
    }
    ListOffsetsRequest.Builder.forReplica(
      org.apache.kafka.server.common.MetadataVersion.latestTesting().listOffsetRequestVersion,
      brokerId
    ).setTargetTimes(topics)
  }

  private def parseListOffsetsResponse(response: ListOffsetsResponse): Map[TopicPartition, Long] = {
    val result = scala.collection.mutable.Map[TopicPartition, Long]()
    response.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        if (partition.errorCode == 0) {
          val tp = new TopicPartition(topic.name, partition.partitionIndex)
          result.put(tp, partition.offset)
        }
      }
    }
    result.toMap
  }
}