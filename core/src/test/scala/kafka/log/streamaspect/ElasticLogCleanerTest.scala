package kafka.log.streamaspect

import com.automq.stream.api.Client
import kafka.log.streamaspect.client.Context
import kafka.log.{CleanedTransactionMetadata, CleanerStats, FakeOffsetMap, LogCleanerTest}
import kafka.server.BrokerTopicStats
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.storage.internals.log.{FetchIsolation, LogConfig, LogDirFailureChannel, LogOffsetsListener}
import org.junit.jupiter.api.{Assertions, BeforeEach, Tag, Test, Timeout}

import java.io.File
import java.util.Properties
import scala.jdk.CollectionConverters.CollectionHasAsScala

@Timeout(60)
@Tag("S3Unit")
class ElasticLogCleanerTest extends LogCleanerTest {
  var client: Client = _

  @BeforeEach
  def setUp(): Unit = {
    Context.enableTestMode()
    client = new MemoryClient()
  }

  // TODO: deal with corrupted message
  override def testCleanCorruptMessageSet(): Unit = super.testCleanCorruptMessageSet()

  override def testCorruptMessageSizeLargerThanBytesAvailable(): Unit = {
    // AutoMQ don't have local file
  }

  override def testRecoveryAfterCrash(): Unit = {
    // AutoMQ will ignore .cleaned Segment when recover
  }

  // TODO: support log reload
  override def testDuplicateCheckAfterCleaning(): Unit = super.testDuplicateCheckAfterCleaning()

  override def testSegmentWithOffsetOverflow(): Unit = {
    // AutoMQ won't have offset overflow
  }

  override def testMessageLargerThanMaxMessageSizeWithCorruptHeader(): Unit = {
    // AutoMQ don't have local file
  }

  @Test
  def testCleanSegmentCauseHollow(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    log.appendAsLeader(record(1, 1), leaderEpoch = 0)
    log.appendAsLeader(record(2, 1), leaderEpoch = 0)
    while (log.numberOfSegments < 3)
      log.appendAsLeader(record(1, log.logEndOffset.toInt), leaderEpoch = 0)

    val map = new FakeOffsetMap(Int.MaxValue)
    map.put(key(2L), 1L)
    map.put(key(1L), log.logEndOffset - 1)

    // after the clean
    // 0.clean: RecordBatch{offset=0, 2L=1L, count=2}
    // there is hollow between segment 0.clean and next segment
    val segments = log.logSegments.asScala.take(1).toSeq
    val stats = new CleanerStats()
    cleaner.cleanSegments(log, segments, map, 0L, stats, new CleanedTransactionMetadata, -1)

    var offset = 0L
    while (offset < log.logEndOffset) {
      val rst = log.read(offset, 1, FetchIsolation.LOG_END, minOneMessage = true)
      Assertions.assertNotNull(rst)
      Assertions.assertTrue(rst.records.sizeInBytes() > 0)
      rst.records.records().forEach(_ => {
        offset += 1
      })
    }
  }

  @Test
  @Timeout(value = 30)
  def testCleanSegmentCauseHollowWithEmptySegment(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    log.appendAsLeader(record(1, 0), leaderEpoch = 0)
    log.appendAsLeader(record(2, 0), leaderEpoch = 0)
    while (log.numberOfSegments < 2) {
      log.appendAsLeader(record(1, log.logEndOffset.toInt), leaderEpoch = 0)
    }
    while (log.numberOfSegments < 3) {
      log.appendAsLeader(record(3, 22), leaderEpoch = 0)
    }
    log.appendAsLeader(record(1, log.logEndOffset.toInt), leaderEpoch = 0)
    log.appendAsLeader(record(3, log.logEndOffset.toInt), leaderEpoch = 0)

    val map = new FakeOffsetMap(Int.MaxValue)
    map.put(key(2L), 1L)
    map.put(key(1L), log.logEndOffset - 2)
    map.put(key(3L), log.logEndOffset - 1)

    // create an empty segment in between first and last segment
    cleaner.cleanSegments(log, log.logSegments.asScala.take(1).toSeq, map, 0L, new CleanerStats, new CleanedTransactionMetadata, -1)
    cleaner.cleanSegments(log, log.logSegments.asScala.slice(1, 2).toSeq, map, 0L, new CleanerStats, new CleanedTransactionMetadata, -1)

    log.logSegments.asScala.slice(1, 2).foreach(s => {
      Assertions.assertEquals(0, s.size())
    })

    var offset = 0L
    var total = 0
    while (offset < log.logEndOffset) {
      val rst = log.read(offset, 1, FetchIsolation.LOG_END, minOneMessage = true)
      Assertions.assertNotNull(rst)
      rst.records.batches.forEach(b => {
        total += 1
        offset = b.nextOffset()
      })
    }
    Assertions.assertEquals(4, total)
  }

  override protected def makeLog(dir: File,
    config: LogConfig,
    recoveryPoint: Long): ElasticUnifiedLog = {
    ElasticUnifiedLog.apply(
      dir,
      config,
      time.scheduler,
      time,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig,
      brokerTopicStats = new BrokerTopicStats,
      producerIdExpirationCheckIntervalMs = TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
      new LogDirFailureChannel(10),
      topicId = Some(Uuid.ZERO_UUID),
      0,
      logOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER,
      client,
      "test_namespace",
      OpenStreamChecker.NOOP,
    )
  }
}
