package unit.kafka.server.streamaspect

import com.google.common.util.concurrent.MoreExecutors
import kafka.automq.AutoMQConfig
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.log.remote.RemoteLogManager
import kafka.log.streamaspect.ElasticLogManager
import kafka.log.{LocalLog, LogLoader, LogManager, UnifiedLog}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.server.epoch.util.MockBlockingSender
import kafka.server.streamaspect.ElasticReplicaManager
import kafka.utils.{Pool, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.es.ElasticStreamSwitch
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{DirectoryId, Node, TopicPartition, Uuid}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.server.common.automq.AutoMQVersion
import org.apache.kafka.server.common.{DirectoryEventHandler, OffsetAndEpoch}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.util.timer.MockTimer
import org.apache.kafka.server.util.{MockScheduler, Scheduler}
import org.apache.kafka.storage.internals.log._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Disabled, Tag, Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, when}

import java.io.File
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.{Map, Seq}
import scala.compat.java8.OptionConverters.RichOptionForJava8
import scala.jdk.CollectionConverters.{MapHasAsJava, PropertiesHasAsScala}

@Timeout(60)
@Tag("S3Unit")
class ElasticReplicaManagerTest extends ReplicaManagerTest {

  @BeforeEach
  override def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put(AutoMQConfig.ELASTIC_STREAM_ENABLE_CONFIG, true)
    props.put(AutoMQConfig.ELASTIC_STREAM_ENDPOINT_CONFIG, "memory://")
    config = KafkaConfig.fromProps(props)
    alterPartitionManager = mock(classOf[AlterPartitionManager])
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
    mockRemoteLogManager = mock(classOf[RemoteLogManager])
    addPartitionsToTxnManager = mock(classOf[AddPartitionsToTxnManager])

    // Anytime we try to verify, just automatically run the callback as though the transaction was verified.
    when(addPartitionsToTxnManager.verifyTransaction(any(), any(), any(), any(), any(), any())).thenAnswer { invocationOnMock =>
      val callback = invocationOnMock.getArgument(4, classOf[AddPartitionsToTxnManager.AppendCallback])
      callback(Map.empty[TopicPartition, Errors].toMap)
    }

    // Set the default value for the elastic storage system enable config
    ElasticStreamSwitch.setSwitch(true)
    ElasticLogManager.enable(true)
    ElasticLogManager.init(config, "clusterId", null)
  }

  override protected def setUpReplicaManager(config: KafkaConfig,
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
    directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP) = new ElasticReplicaManager(config, metrics, time, scheduler, logManager, remoteLogManager, quotaManagers,
      metadataCache, logDirFailureChannel, alterPartitionManager, brokerTopicStats, isShuttingDown, zkClient,
      delayedProducePurgatoryParam, delayedFetchPurgatoryParam, delayedDeleteRecordsPurgatoryParam,
      delayedElectLeaderPurgatoryParam, delayedRemoteFetchPurgatoryParam, threadNamePrefix, brokerEpochSupplier,
      addPartitionsToTxnManager, directoryEventHandler,
      MoreExecutors.newDirectExecutorService(), MoreExecutors.newDirectExecutorService())

  override protected def setUpReplicaManagerWithMockedAddPartitionsToTxnManager(addPartitionsToTxnManager: AddPartitionsToTxnManager,
      transactionalTopicPartitions: List[TopicPartition],
      config: KafkaConfig = config): ReplicaManager = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), time = time)
    val metadataCache = mock(classOf[MetadataCache])

    val replicaManager = setUpReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager))

    transactionalTopicPartitions.foreach(tp => when(metadataCache.contains(tp)).thenReturn(true))

    // We will attempt to schedule to the request handler thread using a non request handler thread. Set this to avoid error.
    KafkaRequestHandler.setBypassThreadCheck(true)
    replicaManager
  }

  override protected def setupReplicaManagerWithMockedPurgatories(
    timer: MockTimer,
    brokerId: Int,
    aliveBrokerIds: collection.Seq[Int],
    propsModifier: Properties => Unit,
    mockReplicaFetcherManager: Option[ReplicaFetcherManager],
    mockReplicaAlterLogDirsManager: Option[ReplicaAlterLogDirsManager],
    isShuttingDown: AtomicBoolean,
    enableRemoteStorage: Boolean,
    shouldMockLog: Boolean, remoteLogManager: Option[RemoteLogManager],
    defaultTopicRemoteLogStorageEnable: Boolean,
    setupLogDirMetaProperties: Boolean,
    directoryEventHandler: DirectoryEventHandler,
    buildRemoteLogAuxState: Boolean,
    remoteFetchQuotaExceeded: Option[Boolean] = None
  ): ElasticReplicaManager = {
    val props = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    val path1 = TestUtils.tempRelativeDir("data").getAbsolutePath
    val path2 = TestUtils.tempRelativeDir("data2").getAbsolutePath
    if (enableRemoteStorage) {
      props.put("log.dirs", path1)
      props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, enableRemoteStorage.toString)
    } else props.put("log.dirs", path1 + "," + path2)
    propsModifier.apply(props)
    val config = KafkaConfig.fromProps(props)
    val logProps = new Properties()
    if (enableRemoteStorage && defaultTopicRemoteLogStorageEnable) logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    val mockLog = setupMockLog(path1)
    if (setupLogDirMetaProperties) {
      // add meta.properties file in each dir
      config.logDirs.foreach(dir => {
        val metaProps = new MetaProperties.Builder().
          setVersion(MetaPropertiesVersion.V0).
          setClusterId("clusterId").
          setNodeId(brokerId).
          setDirectoryId(DirectoryId.random()).
          build()
        PropertiesUtils.writePropertiesFile(metaProps.toProperties,
          new File(new File(dir), MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath, false)
      })
    }
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)), new LogConfig(logProps), log = if (shouldMockLog) Some(mockLog) else None, remoteStorageSystemEnable = enableRemoteStorage, time = time)
    val logConfig = new LogConfig(logProps)
    when(mockLog.config).thenReturn(logConfig)
    when(mockLog.remoteLogEnabled()).thenReturn(enableRemoteStorage)
    when(mockLog.remoteStorageSystemEnable).thenReturn(enableRemoteStorage)
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))
    brokerTopicStats = new BrokerTopicStats()

    val metadataCache = mock(classOf[MetadataCache])
    when(metadataCache.topicIdInfo()).thenReturn((topicIds.asJava, topicNames.asJava))
    when(metadataCache.topicNamesToIds()).thenReturn(topicIds.asJava)
    when(metadataCache.topicIdsToNames()).thenReturn(topicNames.asJava)
    when(metadataCache.metadataVersion()).thenReturn(config.interBrokerProtocolVersion)
    when(metadataCache.autoMQVersion()).thenReturn(AutoMQVersion.LATEST)
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)

    DelayedFetch.setFetchExecutor(MoreExecutors.newDirectExecutorService())
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)

    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockDelayedElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "DelayedElectLeader", timer, reaperEnabled = false)
    val mockDelayedRemoteFetchPurgatory = new DelayedOperationPurgatory[DelayedRemoteFetch](
      purgatoryName = "DelayedRemoteFetch", timer, reaperEnabled = false)

    when(metadataCache.contains(new TopicPartition(topic, 0))).thenReturn(true)

    // Transactional appends attempt to schedule to the request handler thread using a non request handler thread. Set this to avoid error.
    KafkaRequestHandler.setBypassThreadCheck(true)

    new ElasticReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = new MockScheduler(time),
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      metadataCache = metadataCache,
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterPartitionManager,
      brokerTopicStats = brokerTopicStats,
      isShuttingDown = isShuttingDown,
      delayedProducePurgatoryParam = Some(mockProducePurgatory),
      delayedFetchPurgatoryParam = Some(mockFetchPurgatory),
      delayedDeleteRecordsPurgatoryParam = Some(mockDeleteRecordsPurgatory),
      delayedElectLeaderPurgatoryParam = Some(mockDelayedElectLeaderPurgatory),
      delayedRemoteFetchPurgatoryParam = Some(mockDelayedRemoteFetchPurgatory),
      threadNamePrefix = Option(this.getClass.getName),
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager),
      directoryEventHandler = directoryEventHandler,
      remoteLogManager = if (enableRemoteStorage) if (remoteLogManager.isDefined)
        remoteLogManager
      else
        Some(mockRemoteLogManager) else None,
      fastFetchExecutor = MoreExecutors.newDirectExecutorService(),
      slowFetchExecutor = MoreExecutors.newDirectExecutorService()) {

      override protected def createReplicaFetcherManager(
        metrics: Metrics,
        time: Time,
        threadNamePrefix: Option[String],
        quotaManager: ReplicationQuotaManager
      ): ReplicaFetcherManager = mockReplicaFetcherManager.getOrElse {
        if (buildRemoteLogAuxState) {
          super.createReplicaFetcherManager(
            metrics,
            time,
            threadNamePrefix,
            quotaManager
          )
          val config = this.config
          val metadataCache = this.metadataCache
          new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager, () => metadataCache.metadataVersion(), () => 1) {
            override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
              val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
              val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"

              val tp = new TopicPartition(topic, 0)
              val leader = new MockLeaderEndPoint() {
                override def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = Map(tp -> new FetchData().setErrorCode(Errors.OFFSET_MOVED_TO_TIERED_STORAGE.code))
              }
              leader.setLeaderState(tp, PartitionState(leaderEpoch = 0))
              leader.setReplicaPartitionStateCallback(tp => PartitionState(leaderEpoch = 0))

              val fetcher = new ReplicaFetcherThread(threadName, leader, config, failedPartitions, replicaManager,
                quotaManager, "", () => config.interBrokerProtocolVersion)

              val initialFetchState = InitialFetchState(
                topicId = Some(Uuid.randomUuid()),
                leader = leader.brokerEndPoint(),
                currentLeaderEpoch = 0,
                initOffset = 0)

              fetcher.addPartitions(Map(tp -> initialFetchState))

              fetcher
            }
          }
        } else super.createReplicaFetcherManager(
            metrics,
            time,
            threadNamePrefix,
            quotaManager
          )
      }

      override def createReplicaAlterLogDirsManager(
        quotaManager: ReplicationQuotaManager,
        brokerTopicStats: BrokerTopicStats
      ): ReplicaAlterLogDirsManager = mockReplicaAlterLogDirsManager.getOrElse {
        super.createReplicaAlterLogDirsManager(
          quotaManager,
          brokerTopicStats
        )
      }
    }
  }

  /**
   * This method assumes that the test using created ReplicaManager calls
   * ReplicaManager.becomeLeaderOrFollower() once with LeaderAndIsrRequest containing
   * 'leaderEpochInLeaderAndIsr' leader epoch for partition 'topicPartition'.
   */
  override protected def prepareReplicaManagerAndLogManager(timer: MockTimer,
    topicPartition: Int, leaderEpochInLeaderAndIsr: Int, followerBrokerId: Int, leaderBrokerId: Int,
    countDownLatch: CountDownLatch, expectTruncation: Boolean,
    localLogOffset: Option[Long], offsetFromLeader: Long, leaderEpochFromLeader: Int,
    extraProps: Properties,
    topicId: Option[Uuid]): (ReplicaManager, LogManager) = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath)
    props.asScala ++= extraProps.asScala
    val config = KafkaConfig.fromProps(props)
    val logConfig = new LogConfig(new Properties)
    val logDir = new File(new File(config.logDirs.head), s"$topic-$topicPartition")
    Files.createDirectories(logDir.toPath)
    val mockScheduler = new MockScheduler(time)
    val mockBrokerTopicStats = new BrokerTopicStats
    val mockLogDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)
    val tp = new TopicPartition(topic, topicPartition)
    val maxTransactionTimeoutMs = 30000
    val maxProducerIdExpirationMs = 30000
    val segments = new LogSegments(tp)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(logDir, tp, mockLogDirFailureChannel, logConfig.recordVersion, "", None, mockScheduler)
    val producerStateManager = new ProducerStateManager(tp, logDir,
      maxTransactionTimeoutMs, new ProducerStateManagerConfig(maxProducerIdExpirationMs, true), time)
    val offsets = new LogLoader(
      logDir,
      tp,
      logConfig,
      mockScheduler,
      time,
      mockLogDirFailureChannel,
      hadCleanShutdown = true,
      segments,
      0L,
      0L,
      leaderEpochCache.asJava,
      producerStateManager
    ).load()
    val localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockScheduler, time, tp, mockLogDirFailureChannel)
    val mockLog = new UnifiedLog(
      logStartOffset = offsets.logStartOffset,
      localLog = localLog,
      brokerTopicStats = mockBrokerTopicStats,
      producerIdExpirationCheckIntervalMs = 30000,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = producerStateManager,
      _topicId = topicId,
      keepPartitionMetadataFile = true) {

      override def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
        assertEquals(leaderEpoch, leaderEpochFromLeader)
        localLogOffset.map { logOffset =>
          Some(new OffsetAndEpoch(logOffset, leaderEpochFromLeader))
        }.getOrElse(super.endOffsetForEpoch(leaderEpoch))
      }

      override def latestEpoch: Option[Int] = Some(leaderEpochFromLeader)

      override def logEndOffsetMetadata: LogOffsetMetadata =
        localLogOffset.map(new LogOffsetMetadata(_)).getOrElse(super.logEndOffsetMetadata)

      override def logEndOffset: Long = localLogOffset.getOrElse(super.logEndOffset)
    }

    // Expect to call LogManager.truncateTo exactly once
    val topicPartitionObj = new TopicPartition(topic, topicPartition)
    val mockLogMgr = mock(classOf[LogManager])
    when(mockLogMgr.liveLogDirs).thenReturn(config.logDirs.map(new File(_).getAbsoluteFile))
    when(mockLogMgr.getOrCreateLog(ArgumentMatchers.eq(topicPartitionObj), ArgumentMatchers.eq(false), ArgumentMatchers.eq(false), any(), any(), any())).thenReturn(mockLog)
    when(mockLogMgr.getLog(topicPartitionObj, isFuture = false)).thenReturn(Some(mockLog))
    when(mockLogMgr.getLog(topicPartitionObj, isFuture = true)).thenReturn(None)
    val allLogs = new Pool[TopicPartition, UnifiedLog]()
    allLogs.put(topicPartitionObj, mockLog)
    when(mockLogMgr.allLogs).thenReturn(allLogs.values)
    when(mockLogMgr.isLogDirOnline(anyString)).thenReturn(true)

    val aliveBrokerIds = Seq[Integer](followerBrokerId, leaderBrokerId)
    val aliveBrokers = aliveBrokerIds.map(brokerId => new Node(brokerId, s"host$brokerId", brokerId))

    val metadataCache = mock(classOf[MetadataCache])
    mockGetAliveBrokerFunctions(metadataCache, aliveBrokers)
    when(metadataCache.getPartitionReplicaEndpoints(
      any[TopicPartition], any[ListenerName])).
      thenReturn(Map(leaderBrokerId -> new Node(leaderBrokerId, "host1", 9092, "rack-a"),
        followerBrokerId -> new Node(followerBrokerId, "host2", 9092, "rack-b")).toMap)
    when(metadataCache.metadataVersion()).thenReturn(config.interBrokerProtocolVersion)
    when(metadataCache.autoMQVersion()).thenReturn(AutoMQVersion.LATEST)
    val mockProducePurgatory = new DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", timer, reaperEnabled = false)
    val mockFetchPurgatory = new DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", timer, reaperEnabled = false)
    val mockDeleteRecordsPurgatory = new DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", timer, reaperEnabled = false)
    val mockElectLeaderPurgatory = new DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "ElectLeader", timer, reaperEnabled = false)
    val mockRemoteFetchPurgatory = new DelayedOperationPurgatory[DelayedRemoteFetch](
      purgatoryName = "RemoteFetch", timer, reaperEnabled = false)

    // Mock network client to show leader offset of 5
    val blockingSend = new MockBlockingSender(
      Map(topicPartitionObj -> new EpochEndOffset()
        .setPartition(topicPartitionObj.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochFromLeader)
        .setEndOffset(offsetFromLeader)).asJava,
      BrokerEndPoint(1, "host1", 1), time)
    val replicaManager = new ElasticReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = mockScheduler,
      logManager = mockLogMgr,
      quotaManagers = quotaManager,
      brokerTopicStats = mockBrokerTopicStats,
      metadataCache = metadataCache,
      logDirFailureChannel = mockLogDirFailureChannel,
      alterPartitionManager = alterPartitionManager,
      delayedProducePurgatoryParam = Some(mockProducePurgatory),
      delayedFetchPurgatoryParam = Some(mockFetchPurgatory),
      delayedDeleteRecordsPurgatoryParam = Some(mockDeleteRecordsPurgatory),
      delayedElectLeaderPurgatoryParam = Some(mockElectLeaderPurgatory),
      delayedRemoteFetchPurgatoryParam = Some(mockRemoteFetchPurgatory),
      threadNamePrefix = Option(this.getClass.getName),
      fastFetchExecutor = MoreExecutors.newDirectExecutorService(),
      slowFetchExecutor = MoreExecutors.newDirectExecutorService()) {

      override protected def createReplicaFetcherManager(metrics: Metrics,
        time: Time,
        threadNamePrefix: Option[String],
        replicationQuotaManager: ReplicationQuotaManager): ReplicaFetcherManager = {
        val rm = this
        new ReplicaFetcherManager(this.config, rm, metrics, time, threadNamePrefix, replicationQuotaManager, () => this.metadataCache.metadataVersion(), () => 1) {

          override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
            val logContext = new LogContext(s"[ReplicaFetcher replicaId=${rm.config.brokerId}, leaderId=${sourceBroker.id}, " +
              s"fetcherId=$fetcherId] ")
            val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
            val leader = new RemoteLeaderEndPoint(logContext.logPrefix, blockingSend, fetchSessionHandler, rm.config,
              rm, quotaManager.follower, () => rm.config.interBrokerProtocolVersion, () => 1)
            new ReplicaFetcherThread(s"ReplicaFetcherThread-$fetcherId", leader, rm.config, failedPartitions, rm,
              quotaManager.follower, logContext.logPrefix, () => rm.config.interBrokerProtocolVersion) {
              override def doWork(): Unit = {
                // In case the thread starts before the partition is added by AbstractFetcherManager,
                // add it here (it's a no-op if already added)
                val initialOffset = InitialFetchState(
                  topicId = topicId,
                  leader = new BrokerEndPoint(0, "localhost", 9092),
                  initOffset = 0L, currentLeaderEpoch = leaderEpochInLeaderAndIsr)
                addPartitions(Map(new TopicPartition(topic, topicPartition) -> initialOffset))
                super.doWork()

                // Shut the thread down after one iteration to avoid double-counting truncations
                initiateShutdown()
                countDownLatch.countDown()
              }
            }
          }
        }
      }
    }

    (replicaManager, mockLogMgr)
  }

  @Test
  override def testReplicaNotAvailable(): Unit = {

    def createReplicaManager() = {
      val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
      val config = KafkaConfig.fromProps(props)
      val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
      new ElasticReplicaManager(
        metrics = metrics,
        config = config,
        time = time,
        scheduler = new MockScheduler(time),
        logManager = mockLogMgr,
        quotaManagers = quotaManager,
        metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
        alterPartitionManager = alterPartitionManager,
        fastFetchExecutor = MoreExecutors.newDirectExecutorService(),
        slowFetchExecutor = MoreExecutors.newDirectExecutorService()) {
        override def getPartitionOrException(topicPartition: TopicPartition): Partition = throw Errors.NOT_LEADER_OR_FOLLOWER.exception()
      }
    }

    val replicaManager = createReplicaManager()
    try {
      val tp = new TopicPartition(topic, 0)
      val dir = replicaManager.logManager.liveLogDirs.head.getAbsolutePath
      val errors = replicaManager.alterReplicaLogDirs(Map(tp -> dir))
      assertEquals(Errors.REPLICA_NOT_AVAILABLE, errors(tp))
    } finally replicaManager.shutdown(checkpointHW = false)
  }

  // Disable test preferred fetching
  @Test
  @Disabled
  override def testFetchShouldReturnImmediatelyWhenPreferredReadReplicaIsDefined(): Unit = {
  }

  @Test
  @Disabled
  override def testPreferredReplicaAsLeaderWhenSameRackFollowerIsOutOfIsr(): Unit = {
  }

  // Disable test delta between leader and follower
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaToLeaderOrFollowerMarksPartitionOfflineIfLogCantBeCreated(isStartIdLeader: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaFollowerToNotReplica(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaFollowerWithNoChange(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaToFollowerCompletesFetch(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaFollowerRemovedTopic(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaToFollowerCompletesProduce(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaFollowerStopFetcherBeforeCreatingInitialFetchOffset(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaFromLeaderToFollower(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testDeltaFromFollowerToLeader(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testReplicasAreStoppedWhileInControlledShutdownWithKRaft(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testFetcherAreNotRestartedIfLeaderEpochIsNotBumpedWithKRaftPath(enableRemoteStorage: Boolean): Unit = {
  }

  @Test
  @Disabled
  override def testMaybeAddLogDirFetchersWithoutEpochCache(): Unit = {
  }

  // Test leader only
  @Test
  @Disabled
  override def testFetchFromFollowerShouldNotRunPreferLeaderSelect(): Unit = super.testFetchFromFollowerShouldNotRunPreferLeaderSelect()

  @ParameterizedTest
  @ValueSource(booleans = Array(false))
  @Disabled
  override def testOffsetOutOfRangeExceptionWhenFetchMessages(
    isFromFollower: Boolean): Unit = super.testOffsetOutOfRangeExceptionWhenFetchMessages(isFromFollower)


  // Test with remote storage disable only
  @ParameterizedTest
  @ValueSource(booleans = Array(false))
  @Disabled
  override def testApplyDeltaShouldHandleReplicaAssignedToUnassignedDirectory(enableRemoteStorage: Boolean) = super.testApplyDeltaShouldHandleReplicaAssignedToUnassignedDirectory(enableRemoteStorage)

  @ParameterizedTest
  @ValueSource(booleans = Array(false))
  @Disabled
  override def testStopReplicaWithInexistentPartitionAndPartitionsDelete(enableRemoteStorage: Boolean) = super.testStopReplicaWithInexistentPartitionAndPartitionsDelete(enableRemoteStorage)

  @ParameterizedTest
  @ValueSource(booleans = Array(false))
  override def testDeltaLeaderToNotReplica(enableRemoteStorage: Boolean) = super.testDeltaLeaderToNotReplica(enableRemoteStorage)

  @ParameterizedTest
  @ValueSource(booleans = Array(false))
  @Disabled
  override def testDeltaLeaderToRemovedTopic(enableRemoteStorage: Boolean): Unit = {
  }

  override def testStopReplicaWithExistingPartition(
    leaderEpoch: Int,
    deletePartition: Boolean,
    throwIOException: Boolean,
    expectedOutput: Errors,
    enableRemoteStorage: Boolean): Unit = super.testStopReplicaWithExistingPartition(leaderEpoch, deletePartition, throwIOException, expectedOutput, enableRemoteStorage = false)

  override def testStopReplicaWithInexistentPartition(
    deletePartitions: Boolean,
    throwIOException: Boolean,
    enableRemoteStorage: Boolean): Unit = super.testStopReplicaWithInexistentPartition(deletePartitions, throwIOException, enableRemoteStorage = false)

  // Disable test kafka tiered storage
  @Test
  @Disabled
  override def testRemoteFetchExpiresPerSecMetric(): Unit = {
  }

  @Test
  @Disabled
  override def testPreferredReplicaAsFollower(): Unit = {
  }

  @Test
  @Disabled
  override def testRemoteLogReaderMetrics(): Unit = {
  }

  @Test
  @Disabled
  override def testFetchBeyondHighWatermark(): Unit = {
  }

  @Test
  @Disabled
  override def testFullLairDuringKRaftMigrationWithTopicRecreations(): Unit = {
  }

  @Test
  @Disabled
  override def testActiveProducerState(): Unit = {
  }

  @Test
  @Disabled
  override def testHighWaterMarkDirectoryMapping(): Unit = {
  }

  @Test
  @Disabled
  override def testFencedErrorCausedByBecomeLeader(): Unit = {
  }

  @Test
  @Disabled
  override def testPartitionMetadataFile(): Unit = {
  }

  @Test
  @Disabled
  override def testSuccessfulBuildRemoteLogAuxStateMetrics(): Unit = {
  }

  @Test
  @Disabled
  override def testClearPurgatoryOnBecomingFollower(): Unit = {
  }

  @Test
  @Disabled
  override def testPartitionMetadataFileCreatedWithExistingLog(): Unit = {
  }

  @Test
  @Disabled
  override def testFetchMessagesWhenNotFollowerForOnePartition(): Unit = {
  }

  @Test
  @Disabled
  override def testPartitionListener(): Unit = {
  }

  @Test
  @Disabled
  override def testReadCommittedFetchLimitedAtLSO(): Unit = {
  }

  @Test
  @Disabled
  override def testPartitionMetadataFileCreatedAfterPreviousRequestWithoutIds(): Unit = {
  }

  @Test
  @Disabled
  override def testClearProducePurgatoryOnStopReplica(): Unit = {
  }

  @Test
  @Disabled
  override def testHighwaterMarkRelativeDirectoryMapping(): Unit = {
  }

  @Test
  @Disabled
  override def testOldFollowerLosesMetricsWhenReassignPartitions(): Unit = {
  }

  @Test
  @Disabled
  override def testOldLeaderLosesMetricsWhenReassignPartitions(): Unit = {
  }

  @Test
  @Disabled
  override def testFetcherAreNotRestartedIfLeaderEpochIsNotBumpedWithZkPath(): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testReplicaAlterLogDirsWithAndWithoutIds(usesTopicIds: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testApplyDeltaShouldHandleReplicaAssignedToOnlineDirectory(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testStopReplicaWithInexistentPartitionAndPartitionsDeleteAndIOException(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testApplyDeltaShouldHandleReplicaAssignedToLostDirectory(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testStopReplicaWithDeletePartitionAndExistingPartitionAndNewerLeaderEpochAndIOException(enableRemoteStorage: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testPartitionMarkedOfflineIfLogCantBeCreated(becomeLeader: Boolean): Unit = {
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  @Disabled
  override def testPartitionFetchStateUpdatesWithTopicIdChanges(startsWithTopicId: Boolean): Unit = {
  }

  @Test
  @Disabled
  override def testFailedBuildRemoteLogAuxStateMetrics(): Unit = super.testFailedBuildRemoteLogAuxStateMetrics()

  override def testBuildRemoteLogAuxStateMetricsThrowsException(): Unit = {
    // AutoMQ embedded tiered storage in S3Stream
  }

  override def testRemoteReadQuotaNotExceeded(): Unit = {
    // AutoMQ embedded tiered storage in S3Stream
  }

  override def testRemoteReadQuotaExceeded(): Unit = {
    // AutoMQ embedded tiered storage in S3Stream
  }
}
