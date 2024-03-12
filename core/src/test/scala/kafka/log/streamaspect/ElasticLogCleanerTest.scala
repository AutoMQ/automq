package kafka.log.streamaspect

import com.automq.stream.api.Client
import kafka.log.streamaspect.client.Context
import kafka.log.{LogCleanerTest, UnifiedLog}
import kafka.server.BrokerTopicStats
import org.apache.kafka.common.Uuid
import org.apache.kafka.server.config.Defaults
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, LogOffsetsListener}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.File

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
    override def testSegmentGrouping(): Unit = super.testSegmentGrouping()

    override protected def makeLog(dir: File,
        config: LogConfig,
        recoveryPoint: Long): ElasticUnifiedLog = {
        val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
        ElasticUnifiedLog.apply(topicPartition,
            dir,
            config,
            time.scheduler,
            brokerTopicStats = new BrokerTopicStats,
            time,
            maxTransactionTimeoutMs = 5 * 60 * 1000,
            producerStateManagerConfig,
            producerIdExpirationCheckIntervalMs = Defaults.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS,
            new LogDirFailureChannel(10),
            topicId = Some(Uuid.ZERO_UUID),
            0,
            logOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER,
            client,
            "test_namespace",
        )
    }
}
