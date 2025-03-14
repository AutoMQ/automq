package kafka.server.streamaspect

import com.automq.stream.api.exceptions.FastReadFailFastException
import com.automq.stream.s3.metrics.{MetricsLevel, TimerUtil}
import com.automq.stream.utils.FutureUtil
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor
import kafka.automq.kafkalinking.KafkaLinkingManager
import kafka.automq.partition.snapshot.PartitionSnapshotsManager
import kafka.cluster.Partition
import kafka.log.remote.RemoteLogManager
import kafka.log.streamaspect.{ElasticLogManager, OpenHint, PartitionStatusTracker, ReadHint}
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.Limiter.Handler
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.ReplicaManager.createLogReadResult
import kafka.server._
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpoints}
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.{CoreUtils, Exit}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.errors._
import org.apache.kafka.common.errors.s3.StreamFencedException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, PooledRecords, PooledResource}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.s3.{AutomqGetPartitionSnapshotRequest, AutomqGetPartitionSnapshotResponse}
import org.apache.kafka.common.utils.{ThreadUtils, Time}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.{LocalReplicaChanges, MetadataImage, TopicsDelta}
import org.apache.kafka.metadata.LeaderConstants
import org.apache.kafka.server.common.{DirectoryEventHandler, MetadataVersion}
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsConstants._
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log._

import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.function.{BiFunction, Consumer}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}
import scala.compat.java8.OptionConverters
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala, SetHasAsJava}

object ElasticReplicaManager {
  def emptyReadResults(partitions: Seq[TopicIdPartition]): Seq[(TopicIdPartition, LogReadResult)] = {
    partitions.map(tp => tp -> createLogReadResult(null))
  }

  def createLogReadResult(e: Throwable): LogReadResult = {
    LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      divergingEpoch = None,
      highWatermark = UnifiedLog.UnknownOffset,
      leaderLogStartOffset = UnifiedLog.UnknownOffset,
      leaderLogEndOffset = UnifiedLog.UnknownOffset,
      followerLogStartOffset = UnifiedLog.UnknownOffset,
      fetchTimeMs = -1L,
      lastStableOffset = None,
      exception = Option(e))
  }
}

class ElasticReplicaManager(
  config: KafkaConfig,
  metrics: Metrics,
  time: Time,
  scheduler: Scheduler,
  logManager: LogManager,
  remoteLogManager: Option[RemoteLogManager] = None,
  quotaManagers: QuotaManagers,
  metadataCache: MetadataCache,
  logDirFailureChannel: LogDirFailureChannel,
  alterPartitionManager: AlterPartitionManager,
  brokerTopicStats: BrokerTopicStats = new BrokerTopicStats(),
  isShuttingDown: AtomicBoolean = new AtomicBoolean(false),
  zkClient: Option[KafkaZkClient] = None,
  delayedProducePurgatoryParam: Option[DelayedOperationPurgatory[DelayedProduce]] = None,
  delayedFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedFetch]] = None,
  delayedDeleteRecordsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedDeleteRecords]] = None,
  delayedElectLeaderPurgatoryParam: Option[DelayedOperationPurgatory[DelayedElectLeader]] = None,
  delayedRemoteFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteFetch]] = None,
  threadNamePrefix: Option[String] = None,
  brokerEpochSupplier: () => Long = () => -1,
  addPartitionsToTxnManager: Option[AddPartitionsToTxnManager] = None,
  directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP,
  private val fastFetchExecutor: ExecutorService = S3StreamThreadPoolMonitor.createAndMonitor(4, 4, 0L, TimeUnit.MILLISECONDS, "kafka-apis-fast-fetch-executor", true, 10000),
  private val slowFetchExecutor: ExecutorService = S3StreamThreadPoolMonitor.createAndMonitor(12, 12, 0L, TimeUnit.MILLISECONDS, "kafka-apis-slow-fetch-executor", true, 10000),
  private val partitionMetricsCleanerExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("kafka-partition-metrics-cleaner", true)),
) extends ReplicaManager(config, metrics, time, scheduler, logManager, remoteLogManager, quotaManagers, metadataCache,
  logDirFailureChannel, alterPartitionManager, brokerTopicStats, isShuttingDown, zkClient, delayedProducePurgatoryParam,
  delayedFetchPurgatoryParam, delayedDeleteRecordsPurgatoryParam, delayedElectLeaderPurgatoryParam,
  delayedRemoteFetchPurgatoryParam, threadNamePrefix, brokerEpochSupplier, addPartitionsToTxnManager,
  directoryEventHandler) {

  partitionMetricsCleanerExecutor.scheduleAtFixedRate(() => {
    brokerTopicStats.removeRedundantMetrics(allPartitions.keys)
  }, 1, 1, TimeUnit.HOURS)

  protected val openingPartitions = new ConcurrentHashMap[TopicPartition, CompletableFuture[Void]]()
  protected val closingPartitions = new ConcurrentHashMap[TopicPartition, CompletableFuture[Void]]()

  private val fetchExecutorQueueSizeGaugeMap = new util.HashMap[String, Integer]()
  S3StreamKafkaMetricsManager.setFetchPendingTaskNumSupplier(() => {
    fetchExecutorQueueSizeGaugeMap.put(FETCH_EXECUTOR_FAST_NAME, fastFetchExecutor match {
      case executor: ThreadPoolExecutor => executor.getQueue.size()
      case _ => 0
    })
    fetchExecutorQueueSizeGaugeMap.put(FETCH_EXECUTOR_SLOW_NAME, slowFetchExecutor match {
      case executor: ThreadPoolExecutor => executor.getQueue.size()
      case _ => 0
    })
    fetchExecutorQueueSizeGaugeMap.put(FETCH_EXECUTOR_DELAYED_NAME, DelayedFetch.executorQueueSize)
    fetchExecutorQueueSizeGaugeMap
  })

  private val fastFetchLimiter = new FairLimiter(200 * 1024 * 1024, FETCH_LIMITER_FAST_NAME) // 200MiB
  private val slowFetchLimiter = new FairLimiter(200 * 1024 * 1024, FETCH_LIMITER_SLOW_NAME) // 200MiB
  private val fetchLimiterWaitingTasksGaugeMap = new util.HashMap[String, Integer]()
  S3StreamKafkaMetricsManager.setFetchLimiterWaitingTaskNumSupplier(() => {
    fetchLimiterWaitingTasksGaugeMap.put(FETCH_LIMITER_FAST_NAME, fastFetchLimiter.waitingThreads())
    fetchLimiterWaitingTasksGaugeMap.put(FETCH_LIMITER_SLOW_NAME, slowFetchLimiter.waitingThreads())
    fetchLimiterWaitingTasksGaugeMap
  })
  private val fetchLimiterPermitsGaugeMap = new util.HashMap[String, Integer]()
  S3StreamKafkaMetricsManager.setFetchLimiterPermitNumSupplier(() => {
    fetchLimiterPermitsGaugeMap.put(FETCH_LIMITER_FAST_NAME, fastFetchLimiter.availablePermits())
    fetchLimiterPermitsGaugeMap.put(FETCH_LIMITER_SLOW_NAME, slowFetchLimiter.availablePermits())
    fetchLimiterPermitsGaugeMap
  })
  private val fetchLimiterTimeoutCounterMap = util.Map.of(
    fastFetchLimiter.name, S3StreamKafkaMetricsManager.buildFetchLimiterTimeoutMetric(fastFetchLimiter.name),
    slowFetchLimiter.name, S3StreamKafkaMetricsManager.buildFetchLimiterTimeoutMetric(slowFetchLimiter.name)
  )
  private val fetchLimiterTimeHistogramMap = util.Map.of(
    fastFetchLimiter.name, S3StreamKafkaMetricsManager.buildFetchLimiterTimeMetric(MetricsLevel.INFO, fastFetchLimiter.name),
    slowFetchLimiter.name, S3StreamKafkaMetricsManager.buildFetchLimiterTimeMetric(MetricsLevel.INFO, slowFetchLimiter.name)
  )

  /**
   * Used to reduce allocation in [[readFromLocalLogV2]]
   */
  private val readFutureBuffer = new ThreadLocal[ArrayBuffer[CompletableFuture[Void]]] {
    override def initialValue(): ArrayBuffer[CompletableFuture[Void]] = new ArrayBuffer[CompletableFuture[Void]]()
  }

  private val transactionWaitingForValidationMap = new ConcurrentHashMap[Long, Verification]
  private val lastTransactionCleanTimestamp = new AtomicLong(time.milliseconds())

  private val partitionStatusTracker = new PartitionStatusTracker(time, tp => alterPartitionManager.tryElectLeader(tp))

  private class ElasticLogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean)
    extends LogDirFailureHandler(name, haltBrokerOnDirFailure) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }

      if (ElasticLogManager.enabled()) {
        handlePartitionFailure(newOfflineLogDir)
      } else {
        handleLogDirFailure(newOfflineLogDir)
      }
    }
  }

  /**
   * Partition operation executor, used to execute partition operations in parallel.
   */
  private val partitionOpenOpExecutor = ThreadUtils.newCachedThread(128, "partition_open_op_%d", true)
  private val partitionCloseOpExecutor = ThreadUtils.newCachedThread(128, "partition_close_op_%d", true)
  /**
   * Partition operation map, used to make sure that only one operation is executed for a partition at the same time.
   * It should be modified with [[replicaStateChangeLock]] held.
   */
  private val partitionOpMap = new ConcurrentHashMap[TopicPartition, CompletableFuture[Void]]()

  private var fenced: Boolean = false

  private val partitionLifecycleListeners = new util.ArrayList[PartitionLifecycleListener]()

  private var kafkaLinkingManager = Option.empty[KafkaLinkingManager]

  private val partitionSnapshotsManager = new PartitionSnapshotsManager(time)

  private val snapshotReadPartitions = new ConcurrentHashMap[TopicPartition, Partition]()

  addPartitionLifecycleListener(new PartitionLifecycleListener {
    override def onOpen(partition: Partition): Unit = partitionSnapshotsManager.onPartitionOpen(partition)

    override def onClose(partition: Partition): Unit = partitionSnapshotsManager.onPartitionClose(partition)
  })

  override def startup(): Unit = {
    super.startup()
    val haltBrokerOnFailure = metadataCache.metadataVersion().isLessThan(MetadataVersion.IBP_1_0_IV0)
    logDirFailureHandler = new ElasticLogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  /**
   * Stop the given partitions.
   *
   * @param partitionsToStop set of topic-partitions to be stopped which also indicates whether to remove the
   *                         partition data from the local and remote log storage.
   * @return A map from partitions to exceptions which occurred.
   *         If no errors occurred, the map will be empty.
   */
  override protected def stopPartitions(
    partitionsToStop: collection.Set[StopPartition]): collection.Map[TopicPartition, Throwable] = {
    // First stop fetchers for all partitions.
    val partitions = partitionsToStop.map(_.topicPartition)
    replicaFetcherManager.removeFetcherForPartitions(partitions)
    replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)
    kafkaLinkingManager.foreach(_.removePartitions(partitions.asJava))

    // Second remove deleted partitions from the partition map. Fetchers rely on the
    // ReplicaManager to get Partition's information so they must be stopped first.
    val partitionsToDelete = mutable.Set.empty[TopicPartition]
    partitionsToStop.foreach { stopPartition =>
      val topicPartition = stopPartition.topicPartition
      if (stopPartition.deleteLocalLog) {
        getPartition(topicPartition) match {
          case hostedPartition: HostedPartition.Online =>
            if (allPartitions.remove(topicPartition, hostedPartition)) {
              notifyPartitionClose(hostedPartition.partition)
              brokerTopicStats.removeMetrics(topicPartition)
              maybeRemoveTopicMetrics(topicPartition.topic)
              // AutoMQ for Kafka inject start
              if (ElasticLogManager.enabled()) {
                // For elastic stream, partition leader alter is triggered by setting isr/replicas.
                // When broker is not response for the partition, we need to close the partition
                // instead of delete the partition.
                val start = System.currentTimeMillis()
                hostedPartition.partition.close()
                info(s"partition $topicPartition is closed, cost ${System.currentTimeMillis() - start} ms")
                if (!metadataCache.autoMQVersion().isReassignmentV1Supported) {
                  // TODO: https://github.com/AutoMQ/automq/issues/1153 add schedule check when leader isn't successfully set
                  alterPartitionManager.tryElectLeader(topicPartition)
                }
              } else {
                // Logs are not deleted here. They are deleted in a single batch later on.
                // This is done to avoid having to checkpoint for every deletions.
                hostedPartition.partition.delete()
              }
              // AutoMQ for Kafka inject end
            }

          case _ =>
        }
        partitionsToDelete += topicPartition
      }
      // If we were the leader, we may have some operations still waiting for completion.
      // We force completion to prevent them from timing out.
      completeDelayedFetchOrProduceRequests(topicPartition)
    }

    // Third delete the logs and checkpoint.
    val errorMap = new mutable.HashMap[TopicPartition, Throwable]()
    val remotePartitionsToStop = partitionsToStop.filter {
      sp => logManager.getLog(sp.topicPartition).exists(unifiedLog => unifiedLog.remoteLogEnabled())
    }
    if (partitionsToDelete.nonEmpty) {
      // AutoMQ for Kafka inject start
      if (!ElasticLogManager.enabled()) {
        // Delete the logs and checkpoint.
        logManager.asyncDelete(partitionsToDelete, isStray = false, (tp, e) => errorMap.put(tp, e))
      }
      // AutoMQ for Kafka inject end
    }
    remoteLogManager.foreach { rlm =>
      // exclude the partitions with offline/error state
      val partitions = remotePartitionsToStop.filterNot(sp => errorMap.contains(sp.topicPartition)).toSet.asJava
      if (!partitions.isEmpty) {
        rlm.stopPartitions(partitions, (tp, e) => errorMap.put(tp, e))
      }
    }
    errorMap
  }

  /**
   * Remove the usage of [[Option]] in [[getPartition]] to avoid allocation
   */
  override def getPartition(topicPartition: TopicPartition): HostedPartition = {
    var partition = allPartitions.get(topicPartition)
    if (partition == null) {
      val p = snapshotReadPartitions.get(topicPartition)
      if (p != null) {
        partition = HostedPartition.Online(p)
      }
    }
    if (null == partition) {
      HostedPartition.None
    } else {
      partition
    }
  }

  def getPartitionWithoutSnapshotRead(topicPartition: TopicPartition): HostedPartition = {
    val partition = allPartitions.get(topicPartition)
    if (partition == null) {
      HostedPartition.None
    } else {
      partition
    }
  }

  /**
   * Remove the usage of [[Either]] in [[getPartitionOrException]] to avoid allocation
   */
  def getPartitionOrExceptionV2(topicPartition: TopicPartition): Partition = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        partition
      case HostedPartition.Offline(partition) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")
      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        throw Errors.NOT_LEADER_OR_FOLLOWER.exception(s"Error while fetching partition state for $topicPartition")
      case HostedPartition.None =>
        throw Errors.UNKNOWN_TOPIC_OR_PARTITION.exception(s"Error while fetching partition state for $topicPartition")
    }
  }

  /**
   * Append the messages to the local replica logs
   */
  override protected def appendToLocalLog(internalTopicsAllowed: Boolean,
    origin: AppendOrigin,
    entriesPerPartition: collection.Map[TopicPartition, MemoryRecords],
    requiredAcks: Short, requestLocal: RequestLocal,
    verificationGuards: collection.Map[TopicPartition, VerificationGuard]): collection.Map[TopicPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled

    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = onlinePartition(topicPartition).map(_.logStartOffset).getOrElse(-1L)
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      if (t.isInstanceOf[OutOfOrderSequenceException]) {
        // The following situation can cause OutOfOrderSequenceException, but it could recover from producer retry:
        // 1. Producer send msg1 to partition1 (on broker0) and the msg1 is inflight (fail after step 3);
        // 2. Partition1 move to broker1, and broker1 expect msg1;
        // 3. Producer send msg2 to partition1 (on broker1); (Producer allow 5 inflight messages)
        // 4. Broker1 expect msg1 but receive msg2, so it will throw OutOfOrderSequenceException;
        // 5. [Recover] Producer resend msg1 and msg2 to broker1, and broker2 move the seq to msg3;
        warn(s"[OUT_OF_ORDER] processing append operation on partition $topicPartition", t)
      } else {
        error(s"Error processing append operation on partition $topicPartition", t)
      }

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.topicPartitionStats(topicPartition).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}")),
          hasCustomErrorMessage = false))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition)
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks, requestLocal,
            verificationGuards.getOrElse(topicPartition, VerificationGuard.SENTINEL))
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicPartitionStats(topicPartition).bytesInRate.mark(records.sizeInBytes())
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
              s"${info.firstOffset} and ending at offset ${info.lastOffset}")

          (topicPartition, LogAppendResult(info, exception = None, hasCustomErrorMessage = false))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@(_: UnknownTopicOrPartitionException |
                  _: NotLeaderOrFollowerException |
                  _: RecordTooLargeException |
                  _: RecordBatchTooLargeException |
                  _: CorruptRecordException |
                  _: KafkaStorageException |
                  _: DuplicateSequenceException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, Some(e), hasCustomErrorMessage = false))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(logStartOffset, recordErrors),
              Some(rve.invalidException), hasCustomErrorMessage = true))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset),
              Some(t), hasCustomErrorMessage = false))
        }
      }
    }
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  override def fetchMessages(params: FetchParams,
    fetchInfos: Seq[(TopicIdPartition, FetchRequest.PartitionData)],
    quota: ReplicaQuota,
    responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit): Unit = {

    def responseEmpty(e: Throwable): Unit = {
      val fetchPartitionData = fetchInfos.map { case (tp, _) =>
        tp -> createLogReadResult(e).toFetchPartitionData(false)
      }
      responseCallback(fetchPartitionData)
    }

    def handleError(e: Throwable): Unit = {
      error(s"Unexpected error handling request ${params} ${fetchInfos} ", e)
      // convert fetchInfos to error Seq[(TopicPartition, FetchPartitionData)] for callback
      responseEmpty(e)
    }

    // The fetching is done is a separate thread pool to avoid blocking io thread.
    fastFetchExecutor.submit(new Runnable {
      override def run(): Unit = {
        try {
          ReadHint.markReadAll()
          ReadHint.markFastRead()
          // no timeout for fast read
          fetchMessages0(params, fetchInfos, quota, fastFetchLimiter, 0, responseCallback)
          ReadHint.clear()
        } catch {
          case e: Throwable =>
            val ex = FutureUtil.cause(e)
            val fastReadFailFast = ex.isInstanceOf[FastReadFailFastException]
            if (fastReadFailFast) {
              val timer = Time.SYSTEM.timer(params.maxWaitMs)
              slowFetchExecutor.submit(new Runnable {
                override def run(): Unit = {
                  try {
                    timer.update()
                    if (timer.isExpired) {
                      // return empty response if timeout
                      responseEmpty(null)
                      return
                    }
                    ReadHint.markReadAll()
                    assert(timer.remainingMs() > 0, "Remaining time should be positive")
                    fetchMessages0(params, fetchInfos, quota, slowFetchLimiter, timer.remainingMs(), responseCallback)
                  } catch {
                    case slowEx: Throwable =>
                      handleError(slowEx)
                  }
                }
              })
            } else {
              error(s"Unexpected error handling request ${params} ${fetchInfos} ", e)
            }
        }
      }
    })
  }

  // Original version of fetchMessages function
  def fetchMessages0(params: FetchParams,
    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
    quota: ReplicaQuota,
    limiter: Limiter,
    timeoutMs: Long,
    responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit): Unit = {

    // check if this fetch request can be satisfied right away
    val logReadResults = readFromLocalLogV2(params, fetchInfos, quota, readFromPurgatory = false, limiter, timeoutMs)
    var bytesReadable: Long = 0
    var errorReadingData = false

    // The 1st topic-partition that has to be read from remote storage
    var remoteFetchInfo: Optional[RemoteStorageFetchInfo] = Optional.empty()

    var hasDivergingEpoch = false
    var hasPreferredReadReplica = false
    val logReadResultMap = new mutable.HashMap[TopicIdPartition, LogReadResult]

    logReadResults.foreach { case (topicIdPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicIdPartition.topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.topicPartitionStats(topicIdPartition.topicPartition).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (!remoteFetchInfo.isPresent && logReadResult.info.delayedRemoteStorageFetch.isPresent) {
        remoteFetchInfo = logReadResult.info.delayedRemoteStorageFetch
      }
      if (logReadResult.divergingEpoch.nonEmpty)
        hasDivergingEpoch = true
      if (logReadResult.preferredReadReplica.nonEmpty)
        hasPreferredReadReplica = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicIdPartition, logReadResult)
    }

    // Respond immediately if no remote fetches are required and any of the below conditions is true
    //                        1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) does not have enough data to respond but it's a catch-up read (this means we reached
    //                           the end of a segment)
    //                        5) some error happens while reading data
    //                        6) we found a diverging epoch
    //                        7) has a preferred read replica
    if (!remoteFetchInfo.isPresent && (params.maxWaitMs <= 0 || fetchInfos.isEmpty
      || (bytesReadable >= params.minBytes || (bytesReadable < params.minBytes && !ReadHint.isFastRead))
      || errorReadingData || hasDivergingEpoch || hasPreferredReadReplica)) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = params.isFromFollower && isAddingReplica(tp.topicPartition, params.replicaId)
        tp -> result.toFetchPartitionData(isReassignmentFetch)
      }
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicIdPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicIdPartition, partitionData) =>
        logReadResultMap.get(topicIdPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicIdPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }

      if (remoteFetchInfo.isPresent) {
        val maybeLogReadResultWithError = processRemoteFetch(remoteFetchInfo.get(), params, responseCallback, logReadResults, fetchPartitionStatus)
        if (maybeLogReadResultWithError.isDefined) {
          // If there is an error in scheduling the remote fetch task, return what we currently have
          // (the data read from local log segment for the other topic-partitions) and an error for the topic-partition
          // that we couldn't read from remote storage
          val partitionToFetchPartitionData = buildPartitionToFetchPartitionData(logReadResults, remoteFetchInfo.get().topicPartition, maybeLogReadResultWithError.get)
          responseCallback(partitionToFetchPartitionData)
        }
      } else {
        // release records before delay fetch
        logReadResults.foreach { case (_, logReadResult) =>
          logReadResult.info.records match {
            case r: PooledResource =>
              r.release()
            case _ =>
          }
        }

        // If there is not enough data to respond and there is no remote data, we will let the fetch request
        // wait for new data.
        val delayedFetch = new DelayedFetch(
          params = params,
          fetchPartitionStatus = fetchPartitionStatus,
          replicaManager = this,
          quota = quota,
          // Always use the fast fetch limiter in delayed fetch operations, as when a delayed fetch
          // operation is completed, it only try to read in the fast path.
          limiter = fastFetchLimiter,
          responseCallback = responseCallback
        )

        // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
        val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

        // try to complete the request immediately, otherwise put it into the purgatory;
        // this is because while the delayed fetch operation is being created, new requests
        // may arrive and hence make this operation completable.
        delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
      }
    }
  }

  /**
   * A Wrapper of [[readFromLocalLogV2]] which acquire memory permits from limiter.
   * It has the same behavior as [[readFromLocalLogV2]] using the default [[NoopLimiter]].
   * A non-positive `timeoutMs` means no timeout.
   */
  def readFromLocalLogV2(
    params: FetchParams,
    readPartitionInfo: Seq[(TopicIdPartition, PartitionData)],
    quota: ReplicaQuota,
    readFromPurgatory: Boolean,
    limiter: Limiter = NoopLimiter.INSTANCE,
    timeoutMs: Long = 0): Seq[(TopicIdPartition, LogReadResult)] = {

    def bytesNeed(): Int = {
      // sum the sizes of topics to fetch from fetchInfos
      val bytesNeed = readPartitionInfo.foldLeft(0) { case (sum, (_, partitionData)) => sum + partitionData.maxBytes }
      val bytesNeedFromParam = if (bytesNeed <= 0) params.maxBytes else math.min(bytesNeed, params.maxBytes)

      // limit the bytes need to half of the maximum permits
      math.min(bytesNeedFromParam, limiter.maxPermits())
    }

    val timer: TimerUtil = new TimerUtil()
    val handler: Handler = timeoutMs match {
      case t if t > 0 => limiter.acquire(bytesNeed(), t)
      case _ => limiter.acquire(bytesNeed())
    }
    fetchLimiterTimeHistogramMap.get(limiter.name).record(timer.elapsedAs(TimeUnit.NANOSECONDS))

    if (handler == null) {
      // the handler will be null if it timed out to acquire from limiter
      fetchLimiterTimeoutCounterMap.get(limiter.name).add(MetricsLevel.INFO, 1)
      // warn(s"Returning emtpy fetch response for fetch request $readPartitionInfo since the wait time exceeds $timeoutMs ms.")
      ElasticReplicaManager.emptyReadResults(readPartitionInfo.map(_._1))
    } else {
      try {
        var logReadResults = readFromLocalLogV2(params, readPartitionInfo, quota, readFromPurgatory)
        if (logReadResults.isEmpty) {
          // release the handler if no logReadResults
          handler.close()
        } else {
          logReadResults.indexWhere(_._2.info.records.sizeInBytes > 0) match {
            case -1 => // no non-empty read result
              handler.close()
            case i => // the first non-empty read result
              // release part of the permits to the actual records size
              val actualRecordsSize = logReadResults.map(_._2.info.records.sizeInBytes).sum
              handler.releaseTo(actualRecordsSize)
              // replace it with a wrapper to release the handler
              val oldReadResult = logReadResults(i)._2
              val oldInfo = oldReadResult.info
              val oldRecords = oldInfo.records
              val newRecords = new PooledRecords(oldRecords, () => handler.close())
              val newInfo = new FetchDataInfo(oldInfo.fetchOffsetMetadata, newRecords, oldInfo.firstEntryIncomplete, oldInfo.abortedTransactions, oldInfo.delayedRemoteStorageFetch)
              val newReadResult = oldReadResult.copy(info = newInfo)
              logReadResults = logReadResults.updated(i, logReadResults(i)._1 -> newReadResult)
          }
        }
        logReadResults
      } catch {
        case e: Throwable =>
          handler.close()
          throw e
      }
    }
  }

  /**
   * Parallel read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLogV2(
    params: FetchParams,
    readPartitionInfo: Seq[(TopicIdPartition, PartitionData)],
    quota: ReplicaQuota,
    readFromPurgatory: Boolean): Seq[(TopicIdPartition, LogReadResult)] = {
    val traceEnabled = isTraceEnabled

    val fastReadFastFail = new AtomicReference[FastReadFailFastException]()

    /**
     * Convert a throwable to [[LogReadResult]] with [[LogReadResult.exception]] set.
     * Note: All parameters except `throwable` are just used for logging or metrics.
     */
    def exception2LogReadResult(throwable: Throwable, tp: TopicIdPartition, fetchInfo: PartitionData,
      limitBytes: Int): LogReadResult = {
      val ex = FutureUtil.cause(throwable)
      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      ex match {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@(_: UnknownTopicOrPartitionException |
                _: NotLeaderOrFollowerException |
                _: UnknownLeaderEpochException |
                _: FencedLeaderEpochException |
                _: ReplicaNotAvailableException |
                _: KafkaStorageException |
                _: OffsetOutOfRangeException |
                _: InconsistentTopicIdException |
                _: FastReadFailFastException) =>
          e match {
            case exception: FastReadFailFastException =>
              fastReadFastFail.set(exception)
            case _ =>
          }
          LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = UnifiedLog.UnknownOffset,
            leaderLogStartOffset = UnifiedLog.UnknownOffset,
            leaderLogEndOffset = UnifiedLog.UnknownOffset,
            followerLogStartOffset = UnifiedLog.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = FetchRequest.describeReplicaId(params.replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = UnifiedLog.UnknownOffset,
            leaderLogStartOffset = UnifiedLog.UnknownOffset,
            leaderLogEndOffset = UnifiedLog.UnknownOffset,
            followerLogStartOffset = UnifiedLog.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e)
          )
      }
    }

    /**
     * Get the partition or throw an exception.
     */
    def getPartitionAndCheckTopicId(tp: TopicIdPartition): Partition = {
      val partition = getPartitionOrExceptionV2(tp.topicPartition)

      // Check if topic ID from the fetch request/session matches the ID in the log
      val topicId = if (tp.topicId == Uuid.ZERO_UUID) None else Some(tp.topicId)
      if (!hasConsistentTopicId(topicId, partition.topicIdV2))
        throw new InconsistentTopicIdException("Topic ID in the fetch session did not match the topic ID in the log.")

      partition
    }

    /**
     * Convert a [[LogReadInfo]] to [[LogReadResult]].
     * This method is just a utility to avoid duplicated code.
     */
    def readInfoToReadResult(
      logReadInfo: LogReadInfo,
      followerLogStartOffset: Long,
      fetchTimeMs: Long,
      fetchDataInfo: FetchDataInfo = null,
      preferredReadReplica: Option[Int] = None,
      exception: Option[Throwable] = None): LogReadResult = {
      val _fetchDataInfo = if (null == fetchDataInfo) {
        logReadInfo.fetchedData
      } else {
        fetchDataInfo
      }
      LogReadResult(info = _fetchDataInfo,
        divergingEpoch = OptionConverters.toScala(logReadInfo.divergingEpoch),
        highWatermark = logReadInfo.highWatermark,
        leaderLogStartOffset = logReadInfo.logStartOffset,
        leaderLogEndOffset = logReadInfo.logEndOffset,
        followerLogStartOffset = followerLogStartOffset,
        fetchTimeMs = fetchTimeMs,
        lastStableOffset = Some(logReadInfo.lastStableOffset),
        preferredReadReplica = preferredReadReplica,
        exception = exception
      )
    }

    def read(partition: Partition, tp: TopicIdPartition, fetchInfo: PartitionData, limitBytes: Int,
      minOneMessage: Boolean): CompletableFuture[LogReadResult] = {
      def _exception2LogReadResult(e: Throwable): LogReadResult = {
        exception2LogReadResult(e, tp, fetchInfo, limitBytes)
      }

      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)

      if (traceEnabled)
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

      val fetchTimeMs = time.milliseconds

      // ~~ If we are the leader, determine the preferred read-replica ~~
      // NOTE: We do not check the preferred read-replica like Apache Kafka does in
      // [[ReplicaManager.readFromLocalLog]], as we always have only one replica per partition.
      val preferredReadReplica = None

      // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
      partition.fetchRecordsAsync(
        fetchParams = params,
        fetchPartitionData = fetchInfo,
        fetchTimeMs = fetchTimeMs,
        maxBytes = adjustedMaxBytes,
        minOneMessage = minOneMessage,
        updateFetchState = !readFromPurgatory
      ).thenApply(readInfo => {
        val fetchDataInfo = if (params.isFromFollower && shouldLeaderThrottle(quota, partition, params.replicaId)) {
          // If the partition is being throttled, simply return an empty set.
          new FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
        } else if (!params.hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
          // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
          // progress in such cases and don't need to report a `RecordTooLargeException`
          new FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
        } else {
          readInfo.fetchedData
        }
        readInfoToReadResult(readInfo, fetchInfo.logStartOffset, fetchTimeMs, fetchDataInfo, preferredReadReplica)
      }).exceptionally(e => _exception2LogReadResult(e))
    }

    val resultInitialSize = Math.min(readPartitionInfo.size, ArrayBuffer.DefaultInitialSize)
    val result = new mutable.ArrayBuffer[(TopicIdPartition, LogReadResult)](resultInitialSize)

    def release(): Unit = {
      result.foreach { case (_, logReadResult) =>
        if (logReadResult.info != null && logReadResult.info.records != null && logReadResult.info.records.isInstanceOf[PooledResource]) {
          logReadResult.info.records.asInstanceOf[PooledResource].release()
        }
      }
    }

    // Note that the use of limitBytes and minOneMessage parameters have been changed here.
    val limitBytes = params.maxBytes
    val minOneMessage = new AtomicBoolean(!params.hardMaxBytesLimit)
    val remainingBytes = new AtomicInteger(limitBytes)

    /**
     * Called when we get a read result. It:
     * - adds the result to the result buffer
     * - updates the minOneMessage flag
     * - decrements the remainingBytes counter
     */
    def handleReadResult(tp: TopicIdPartition, readResult: LogReadResult): Unit = {
      result.synchronized {
        result += (tp -> readResult)
      }
      val recordBatchSize = readResult.info.records.sizeInBytes
      if (recordBatchSize > 0)
        minOneMessage.set(false)
      remainingBytes.getAndAdd(-recordBatchSize)
    }

    var partitionIndex = 0;
    while (remainingBytes.get() > 0 && partitionIndex < readPartitionInfo.size) {
      // In each iteration, we read as many partitions as possible until we reach the maximum bytes limit.
      val readCfArray = readFutureBuffer.get()
      readCfArray.clear()
      var assignedBytes = 0 // The total bytes we have assigned to the read requests.
      val availableBytes = remainingBytes.get() // The remaining bytes we can assign to the read requests, used to control the following loop.

      while (assignedBytes < availableBytes && partitionIndex < readPartitionInfo.size) {
        // Iterate over the partitions.
        val tp = readPartitionInfo(partitionIndex)._1
        val partitionData = readPartitionInfo(partitionIndex)._2
        try {
          val partition = getPartitionAndCheckTopicId(tp)

          val logReadInfo = partition.checkFetchOffsetAndMaybeGetInfo(params, partitionData)
          if (null != logReadInfo) {
            // The fetch offset equals to the confirmed offset, no need to do a read.
            val readResult = readInfoToReadResult(logReadInfo, partitionData.logStartOffset, time.milliseconds)
            handleReadResult(tp, readResult)
          } else {
            // Read from the log.
            val readCf = read(partition, tp, partitionData, partitionData.maxBytes, minOneMessage.get())
            if (readCf.isDone) {
              // If the read is done, we can handle the result immediately.
              val readResult = readCf.get()
              handleReadResult(tp, readResult)
              // As the read is done, we can update the assignedBytes by the actual size of the read result.
              assignedBytes += readResult.info.records.sizeInBytes
            } else {
              // Otherwise, we need to add it to the waiting list.
              readCfArray += readCf.thenAccept(handleReadResult(tp, _))
              assignedBytes += partitionData.maxBytes
            }
          }
        } catch {
          case e: Throwable =>
            val readResult = exception2LogReadResult(e, tp, partitionData, partitionData.maxBytes)
            handleReadResult(tp, readResult)
        }
        partitionIndex += 1
      }
      CompletableFuture.allOf(readCfArray.toArray: _*).get()
    }
    if (fastReadFastFail.get() != null) {
      release()
      throw fastReadFastFail.get()
    }

    // The remaining partitions still need to be read, but we limit byte size to 0.
    // The corresponding futures are completed immediately with empty LogReadResult.
    val remainingCfArray = readFutureBuffer.get()
    remainingCfArray.clear()
    while (partitionIndex < readPartitionInfo.size) {
      val tp = readPartitionInfo(partitionIndex)._1
      val partitionData = readPartitionInfo(partitionIndex)._2
      try {
        val partition = getPartitionAndCheckTopicId(tp)
        // TODO: As the cf will be completed immediately when `limitBytes` set to 0 (see [[ElasticLogSegment.readAsync]]),
        // we can avoid using `CompletableFuture` here.
        val readCf = read(partition, tp, partitionData, 0, minOneMessage.get())
        remainingCfArray += readCf.thenAccept(rst => {
          result.synchronized {
            result += (tp -> rst)
          }
          remainingBytes.getAndAdd(-rst.info.records.sizeInBytes)
        })
      } catch {
        case e: Throwable =>
          val readResult = exception2LogReadResult(e, tp, partitionData, 0)
          handleReadResult(tp, readResult)
      }

      partitionIndex += 1
    }
    CompletableFuture.allOf(remainingCfArray.toArray: _*).get()
    if (fastReadFastFail.get() != null) {
      release()
      throw fastReadFastFail.get()
    }
    result
  }

  def handlePartitionFailure(partitionDir: String): Unit = {
    warn(s"Stopping serving partition $partitionDir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = onlinePartitionsIterator.filter { partition =>
        partition.log.exists {
          _.dir.getAbsolutePath == partitionDir
        }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = onlinePartitionsIterator.filter { partition =>
        partition.futureLog.exists {
          _.dir.getAbsolutePath == partitionDir
        }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))
      kafkaLinkingManager.foreach(_.removePartitions(newOfflinePartitions.asJava))

      // These partitions should first be made offline to remove topic metrics.
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
        brokerTopicStats.removeMetrics(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      // Remove these partitions from the partition map finally.
      allPartitions.removeAll(newOfflinePartitions)

      warn(s"Broker $localBrokerId stopped fetcher for partition ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
        s"for partition ${partitionsWithOfflineFutureReplica.mkString(",")} because it is in the failed log directory $partitionDir.")
    }
    logManager.handlePartitionFailure(partitionDir)

    warn(s"Stopped serving partition replicas in dir $partitionDir")
  }

  private[kafka] def getOrCreatePartitionV2(
    tp: TopicPartition,
    delta: TopicsDelta,
    topicId: Uuid,
    createHook: Consumer[Partition] = _ => {}): Option[(Partition, Boolean)] = {
    getPartitionWithoutSnapshotRead(tp) match {
      case HostedPartition.Offline(offlinePartition) =>
        if (offlinePartition.flatMap(p => p.topicId).contains(topicId)) {
          stateChangeLogger.warn(s"Unable to bring up new local leader $tp " +
            s"with topic id $topicId because it resides in an offline log " +
            "directory.")
          None
        } else {
          stateChangeLogger.info(s"Creating new partition $tp with topic id " + s"$topicId." +
            s"A topic with the same name but different id exists but it resides in an offline log " +
            s"directory.")
          val partition = Partition(tp, time, this)
          allPartitions.put(tp, HostedPartition.Online(partition))
          Some(partition, true)
        }

      case HostedPartition.Online(partition) =>
        if (partition.topicId.exists(_ != topicId)) {
          // Note: Partition#topicId will be None here if the Log object for this partition
          // has not been created.
          throw new IllegalStateException(s"Topic $tp exists, but its ID is " +
            s"${partition.topicId.get}, not $topicId as expected")
        }
        createHook.accept(partition)
        Some(partition, false)

      case HostedPartition.None =>
        // AutoMQ for Kafka inject start
        stateChangeLogger.info(s"Creating new partition $tp with topic id " +
          s"$topicId.")
        //        if (delta.image().topicsById().containsKey(topicId)) {
        //          stateChangeLogger.error(s"Expected partition $tp with topic id " +
        //            s"$topicId to exist, but it was missing. Creating...")
        //        } else {
        //          stateChangeLogger.info(s"Creating new partition $tp with topic id " +
        //            s"$topicId.")
        //        }
        // AutoMQ for Kafka inject end

        // it's a partition that we don't know about yet, so create it and mark it online
        val partition = Partition(tp, time, this)
        createHook.accept(partition)
        allPartitions.put(tp, HostedPartition.Online(partition))
        notifyPartitionOpen(partition)
        Some(partition, true)
    }
  }

  /**
   * Apply a KRaft topic change delta.
   *
   * @param delta    The delta to apply.
   * @param newImage The new metadata image.
   */
  override def applyDelta(delta: TopicsDelta, newImage: MetadataImage): Unit = {
    asyncApplyDelta(delta, newImage, _ => {}).get()
  }

  /**
   * Delete the given partition from the replica manager, if it exists.
   * Note: this method should be called with [[replicaStateChangeLock]] held.
   *
   * @param tp The partition to delete.
   * @return A future which completes when the partition has been deleted.
   */
  private def doPartitionDeletionAsyncLocked(stopPartition: StopPartition): CompletableFuture[Void] = {
    doPartitionDeletionAsyncLocked(stopPartition, _ => {})
  }

  private def doPartitionDeletionAsyncLocked(stopPartition: StopPartition,
    callback: TopicPartition => Unit): CompletableFuture[Void] = {
    val prevOp = partitionOpMap.getOrDefault(stopPartition.topicPartition, CompletableFuture.completedFuture(null))
    val opCf = new CompletableFuture[Void]()
    val tracker = partitionStatusTracker.tracker(stopPartition.topicPartition)
    tracker.expected(PartitionStatusTracker.Status.CLOSED)
    partitionOpMap.put(stopPartition.topicPartition, opCf)
    closingPartitions.put(stopPartition.topicPartition, opCf)
    prevOp.whenComplete((_, _) => {
      partitionCloseOpExecutor.execute(() => {
        try {
          tracker.closing()
          val delete = mutable.Set[StopPartition]()
          delete.add(stopPartition)
          stopPartitions(delete).forKeyValue { (topicPartition, e) =>
            if (e.isInstanceOf[KafkaStorageException]) {
              stateChangeLogger.error(s"Unable to delete replica $topicPartition because " +
                "the local replica for the partition is in an offline log directory")
            } else {
              stateChangeLogger.error(s"Unable to delete replica $topicPartition because " +
                s"we got an unexpected ${e.getClass.getName} exception: ${e.getMessage}")
            }
          }
          callback(stopPartition.topicPartition)
        } finally {
          opCf.complete(null)
          partitionOpMap.remove(stopPartition.topicPartition, opCf)
          closingPartitions.remove(stopPartition.topicPartition, opCf)
          tracker.release()
          tracker.closed()
        }
      })
    })
    opCf
  }

  /**
   * Apply a KRaft topic change delta.
   *
   * @param delta    The delta to apply.
   * @param newImage The new metadata image.
   */
  def asyncApplyDelta(delta: TopicsDelta, newImage: MetadataImage,
    callback: TopicPartition => Unit): CompletableFuture[Void] = {
    // Before taking the lock, compute the local changes
    val localChanges = delta.localChanges(config.nodeId)
    val metadataVersion = newImage.features().metadataVersion()

    val opCfList = new util.LinkedList[CompletableFuture[Void]]()
    val start = System.currentTimeMillis()

    replicaStateChangeLock.synchronized {
      // Handle deleted partitions. We need to do this first because we might subsequently
      // create new partitions with the same names as the ones we are deleting here.
      if (!localChanges.deletes.isEmpty) {
        val deletes = localChanges.deletes.asScala
          .map { tp =>
            val isCurrentLeader = Option(delta.image().getTopic(tp.topic()))
              .map(image => image.partitions().get(tp.partition()))
              .exists(partition => partition.leader == config.nodeId)
            val deleteRemoteLog = delta.topicWasDeleted(tp.topic()) && isCurrentLeader
            StopPartition(tp, deleteLocalLog = true, deleteRemoteLog = deleteRemoteLog)
          }
          .toSet

        def doPartitionDeletion(): Unit = {
          stateChangeLogger.info(s"Deleting ${deletes.size} partition(s).")
          deletes.foreach(stopPartition => {
            val opCf = doPartitionDeletionAsyncLocked(stopPartition, callback)
            opCfList.add(opCf)
          })
        }

        doPartitionDeletion()

        // Clean up the consumption offset for the deleted partitions that do not belong to the current broker.
        delta.deletedTopicIds().forEach { id =>
          val topicImage = delta.image().getTopic(id)
          topicImage.partitions().entrySet().forEach { entry => {
            val partitionId = entry.getKey
            val partition = entry.getValue
            if (partition.leader != config.nodeId) {
              callback(new TopicPartition(topicImage.name(), partitionId))
            }
          }
          }
        }

      }

      // Handle partitions which we are now the leader or follower for.
      if (!localChanges.leaders.isEmpty || !localChanges.followers.isEmpty) {
        val lazyOffsetCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        val leaderChangedPartitions = new mutable.HashSet[Partition]
        val followerChangedPartitions = new mutable.HashSet[Partition]

        val directoryIds = localChanges.directoryIds().asScala

        def doPartitionLeadingOrFollowing(onlyLeaderChange: Boolean): Unit = {
          stateChangeLogger.info(s"Transitioning partition(s) info: $localChanges")
          for (replicas <- Seq(localChanges.leaders, localChanges.followers)) {
            replicas.forEach((tp, info) => {
              val tracker = partitionStatusTracker.tracker(tp)
              tracker.expected(PartitionStatusTracker.Status.LEADER, info.partition().leaderEpoch)
              val prevOp = partitionOpMap.getOrDefault(tp, CompletableFuture.completedFuture(null))
              val opCf = new CompletableFuture[Void]()
              opCfList.add(opCf)
              partitionOpMap.put(tp, opCf)
              openingPartitions.putIfAbsent(tp, opCf)
              prevOp.whenComplete((_, _) => {
                partitionOpenOpExecutor.execute(() => {
                  val leader = mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo]()
                  leader += (tp -> info)
                  try {
                    tracker.opening(info.partition().leaderEpoch)
                    applyLocalLeadersDelta(leaderChangedPartitions, newImage, delta, lazyOffsetCheckpoints, leader, directoryIds)
                    tracker.opened()
                    // Apply the delta before elect leader.
                    callback(tp)
                    // Elect the leader to let client can find the partition by metadata.
                    if (info.partition().leader < 0) {
                      // The tryElectLeader may be failed, tracker will check the partition status and elect leader if needed.
                      alterPartitionManager.tryElectLeader(tp)
                    } else {
                      tracker.leader()
                    }
                  } catch {
                    case t: Throwable => {
                      // If it's a StreamFencedException failure, it's means the partition is reassigned to others.
                      // Expect the tracker will be removed in the following #asyncApplyDelta(TopicsDelta).
                      tracker.failed()
                      val cause = FutureUtil.cause(t)
                      if (cause.isInstanceOf[StreamFencedException]) {
                        stateChangeLogger.warn(s"Transitioning partition(s) fail: $leader", t)
                      } else {
                        stateChangeLogger.error(s"Transitioning partition(s) fail: $leader", t)
                      }
                    }
                  } finally {
                    opCf.complete(null)
                    partitionOpMap.remove(tp, opCf)
                    openingPartitions.remove(tp, opCf)
                    tracker.release()
                  }
                })
              })
            })
          }
          // skip becoming follower or adding log dir
          if (!onlyLeaderChange) {
            if (!localChanges.followers.isEmpty) {
              applyLocalFollowersDelta(followerChangedPartitions, newImage, delta, lazyOffsetCheckpoints, localChanges.followers.asScala, localChanges.directoryIds.asScala)
            }
            maybeAddLogDirFetchers(leaderChangedPartitions ++ followerChangedPartitions, lazyOffsetCheckpoints,
              name => Option(newImage.topics().getTopic(name)).map(_.id()))
          }

          replicaFetcherManager.shutdownIdleFetcherThreads()
          replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

          remoteLogManager.foreach(rlm => rlm.onLeadershipChange(leaderChangedPartitions.asJava, followerChangedPartitions.asJava, localChanges.topicIds()))
        }

        if (ElasticLogManager.enabled()) {
          // When the brokers scale into zero, there isr/replica will set to the last broker.
          // We use fenced state to prevent the last broker re-open partition when the broker shutting down.
          if (!this.fenced) {
            // applyDelta must sync operate, cause of BrokerMetadataPublisher#updateCoordinator will rely on log created.
            doPartitionLeadingOrFollowing(true)
          }
        } else {
          doPartitionLeadingOrFollowing(false)
        }
      }

      CompletableFuture.allOf(opCfList.asScala.toArray: _*).whenComplete((_, _) => {
        if (metadataVersion.isDirectoryAssignmentSupported()) {
          // We only want to update the directoryIds if DirectoryAssignment is supported!
          localChanges.directoryIds.forEach(maybeUpdateTopicAssignment)
        }

        if (!opCfList.isEmpty) {
          val elapsedMs = System.currentTimeMillis() - start
          info(s"open ${localChanges.followers().size()} / make leader ${localChanges.leaders.size()} / close ${localChanges.deletes.size()} partitions cost ${elapsedMs}ms")
        }
      })
    }
  }

  override protected def applyLocalLeadersDelta(
    changedPartitions: mutable.Set[Partition],
    newImage: MetadataImage, delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localLeaders: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo],
    directoryIds: mutable.Map[TopicIdPartition, Uuid]): Unit = {
    stateChangeLogger.info(s"Transitioning ${localLeaders.size} partition(s) to " +
      "local leaders.")
    replicaFetcherManager.removeFetcherForPartitions(localLeaders.keySet)
    localLeaders.forKeyValue { (tp, info) =>
      getOrCreatePartitionV2(tp, delta, info.topicId, partition => {
        val state = info.partition.toLeaderAndIsrPartitionState(tp, true)
        val partitionAssignedDirectoryId = directoryIds.find(_._1.topicPartition() == tp).map(_._2)
        partition.makeLeader(state, offsetCheckpoints, Some(info.topicId), partitionAssignedDirectoryId)
      }).foreach { case (partition, _) =>
        try {
          changedPartitions.add(partition)
          kafkaLinkingManager.foreach(_.addPartitions(Set(partition.topicPartition).asJava))
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.info(s"Skipped the become-leader state change for $tp " +
              s"with topic id ${info.topicId} due to a storage error ${e.getMessage}")
            // If there is an offline log directory, a Partition object may have been created by
            // `getOrCreatePartition()` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(tp)
        }
      }
    }
  }

  override protected def applyLocalFollowersDelta(
    changedPartitions: mutable.Set[Partition],
    newImage: MetadataImage, delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localFollowers: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo],
    directoryIds: mutable.Map[TopicIdPartition, Uuid]): Unit = {
    stateChangeLogger.info(s"Transitioning ${localFollowers.size} partition(s) to " +
      "local followers.")
    val partitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    val partitionsToStopFetching = new mutable.HashMap[TopicPartition, Boolean]
    val followerTopicSet = new mutable.HashSet[String]
    localFollowers.forKeyValue { (tp, info) =>
      getOrCreatePartitionV2(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          followerTopicSet.add(tp.topic)

          // We always update the follower state.
          // - This ensure that a replica with no leader can step down;
          // - This also ensures that the local replica is created even if the leader
          //   is unavailable. This is required to ensure that we include the partition's
          //   high watermark in the checkpoint file (see KAFKA-1647).
          val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
          val partitionAssignedDirectoryId = directoryIds.find(_._1.topicPartition() == tp).map(_._2)
          val isNewLeaderEpoch = partition.makeFollower(state, offsetCheckpoints, Some(info.topicId), partitionAssignedDirectoryId)

          if (isInControlledShutdown && (info.partition.leader == LeaderConstants.NO_LEADER ||
            !info.partition.isr.contains(config.brokerId))) {
            // During controlled shutdown, replica with no leaders and replica
            // where this broker is not in the ISR are stopped.
            partitionsToStopFetching.put(tp, false)
          } else if (isNewLeaderEpoch) {
            // Otherwise, fetcher is restarted if the leader epoch has changed.
            partitionsToStartFetching.put(tp, partition)
          }

          changedPartitions.add(partition)
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Unable to start fetching $tp " +
              s"with topic ID ${info.topicId} due to a storage error ${e.getMessage}", e)
            replicaFetcherManager.addFailedPartition(tp)
            // If there is an offline log directory, a Partition object may have been created by
            // `getOrCreatePartition()` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(tp)

          case e: Throwable =>
            stateChangeLogger.error(s"Unable to start fetching $tp " +
              s"with topic ID ${info.topicId} due to ${e.getClass.getSimpleName}", e)
            replicaFetcherManager.addFailedPartition(tp)
        }
      }
    }
  }

  def awaitAllPartitionShutdown(): Unit = {
    val start = System.currentTimeMillis()
    replicaStateChangeLock.synchronized {
      this.fenced = true
      // When there are any other alive brokers, partitions will be transferred to them and deleted in the method [[asyncApplyDelta]].
      // However when there are no other alive brokers, we need to delete partitions by ourselves here.
      // In summary, a partition may be deleted twice during shutdown. But it is safe as the method [[stopPartitions]]
      // called in [[doPartitionDeletionAsyncLocked]] is idempotent.
      val partitionsToClose = allPartitions.keys
      info(s"stop partitions $partitionsToClose")
      partitionsToClose.foreach(tp => doPartitionDeletionAsyncLocked(StopPartition(tp, deleteLocalLog = true, deleteRemoteLog = true)))
    }
    val closed = checkAllPartitionClosedLoop(30000, 1000)
    if (!closed) {
      warn(s"some partitions are not closed after ${System.currentTimeMillis() - start} ms, they will be force closed")
    } else {
      info(s"all partitions are closed after ${System.currentTimeMillis() - start} ms")
    }
    partitionOpenOpExecutor.shutdown()
    partitionCloseOpExecutor.shutdown()
    CoreUtils.swallow(ElasticLogManager.shutdown(), this)
  }

  /**
   * Check whether all partitions are closed, if not, retry until timeout.
   * If any partition's state changed, the timeout will be reset.
   *
   * @param timeout       timeout in milliseconds
   * @param retryInterval retry interval in milliseconds
   * @return true if all partition closed, false otherwise
   */
  private def checkAllPartitionClosedLoop(timeout: Long, retryInterval: Long): Boolean = {
    var start = System.currentTimeMillis()
    var preAllPartitionCnt = allPartitions.size
    var preClosingPartitionCnt = closingPartitions.size
    var preOpeningPartitionCnt = openingPartitions.size
    while (System.currentTimeMillis() - start < timeout) {
      val curAllPartitionCnt = allPartitions.size
      val curClosingPartitionCnt = closingPartitions.size
      val curOpeningPartitionCnt = openingPartitions.size
      if (curAllPartitionCnt == 0 && curClosingPartitionCnt == 0 && curOpeningPartitionCnt == 0) {
        // all partitions are closed
        return true
      }
      if (curAllPartitionCnt != preAllPartitionCnt || curClosingPartitionCnt != preClosingPartitionCnt || curOpeningPartitionCnt != preOpeningPartitionCnt) {
        // if any partition's state changed, reset the start time
        start = System.currentTimeMillis()
        preAllPartitionCnt = curAllPartitionCnt
        preClosingPartitionCnt = curClosingPartitionCnt
        preOpeningPartitionCnt = curOpeningPartitionCnt
      }
      info(s"still has opening partition, retry check later. online partition: $curAllPartitionCnt, closing partition: $curClosingPartitionCnt, opening partition: $curOpeningPartitionCnt")
      Thread.sleep(retryInterval)
    }
    warn(s"not all partitions are closed. online partition: ${allPartitions.keys}, closing partition: ${closingPartitions.keySet()}, opening partition: ${openingPartitions.keySet()}")
    false
  }

  override def verify(transactionalId: String,
    producerId: Long): Verification = {
    if (transactionalId != null) {
      transactionWaitingForValidationMap.computeIfAbsent(producerId, _ => {
        Verification(
          new AtomicBoolean(false),
          new ArrayBlockingQueue[TransactionVerificationRequest](5),
          new AtomicLong(time.milliseconds()))
      })
    } else {
      null
    }
  }

  override def checkWaitingTransaction(
    verification: Verification,
    task: () => Unit
  ): Boolean = {
    val request = TransactionVerificationRequest(task)
    if (verification != null) {
      verification.synchronized {
        if (!verification.hasInflight.compareAndSet(false, true)) {
          verification.waitingRequests.put(request)
          return true
        }
      }
    }
    false
  }

  override def verifyTransactionCallbackWrapper[T](
    verification: Verification,
    callback: (RequestLocal, T) => Unit,
  ): (RequestLocal, T) => Unit = {
    (requestLocal, args) => {
      try {
        callback(requestLocal, args)
      } catch {
        case e: Throwable =>
          error("Error in transaction verification callback", e)
      }
      if (verification != null) {
        verification.synchronized {
          verification.timestamp.set(time.milliseconds())
          if (!verification.waitingRequests.isEmpty) {
            // Since the callback thread and task thread may be different, we need to ensure that the tasks are executed sequentially.
            val request = verification.waitingRequests.poll()
            request.task()
          } else {
            // If there are no tasks in the queue, set hasInflight to false
            verification.hasInflight.set(false)
          }
        }
        val lastCleanTimestamp = lastTransactionCleanTimestamp.get();
        val now = time.milliseconds()
        if (now - lastCleanTimestamp > 60 * 1000 && lastTransactionCleanTimestamp.compareAndSet(lastCleanTimestamp, now)) {
          transactionWaitingForValidationMap
            .entrySet()
            .removeIf(
              entry => {
                val verification = entry.getValue
                !verification.hasInflight.get() && (now - verification.timestamp.get()) > 60 * 60 * 1000
              })

        }
      }
    }
  }

  def handleGetPartitionSnapshotRequest(request: AutomqGetPartitionSnapshotRequest): AutomqGetPartitionSnapshotResponse = {
    partitionSnapshotsManager.handle(request)
  }

  def addPartitionLifecycleListener(listener: PartitionLifecycleListener): Unit = {
    partitionLifecycleListeners.add(listener)
  }

  def computeSnapshotReadPartition(topicPartition: TopicPartition,
    remappingFunction: BiFunction[TopicPartition, Partition, Partition]): Partition = {
    snapshotReadPartitions.compute(topicPartition, remappingFunction)
  }

  def newSnapshotReadPartition(topicIdPartition: TopicIdPartition): Partition = {
    OpenHint.markSnapshotRead()
    val partition = Partition.apply(topicIdPartition, time, this)
    partition.leaderReplicaIdOpt = Some(localBrokerId)
    partition.createLogIfNotExists(true, false, new LazyOffsetCheckpoints(highWatermarkCheckpoints), partition.topicId, Option.empty)
    OpenHint.clear()
    partition
  }

  private def notifyPartitionOpen(partition: Partition): Unit = {
    partitionLifecycleListeners.forEach(listener => CoreUtils.swallow(listener.onOpen(partition), this))
  }

  private def notifyPartitionClose(partition: Partition): Unit = {
    partitionLifecycleListeners.forEach(listener => CoreUtils.swallow(listener.onClose(partition), this))
  }

  override def shutdown(checkpointHW: Boolean): Unit = {
    kafkaLinkingManager.foreach(_.shutdown())
    super.shutdown(checkpointHW)
  }

  def setKafkaLinkingManager(kafkaLinkingManager: KafkaLinkingManager): Unit = {
      if (kafkaLinkingManager == null) {
          this.kafkaLinkingManager = None
      } else {
          this.kafkaLinkingManager = Some(kafkaLinkingManager)
      }
  }

}
