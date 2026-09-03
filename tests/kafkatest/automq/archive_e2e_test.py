"""
Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
"""

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.version import DEV_BRANCH

from kafkatest.automq.automq_e2e_util import S3_WAL


class ArchiveFetchE2ETest(Test):
    """Verify records remain fetchable after a Composite object is archived."""

    TOPIC = "automq_archive_fetch_e2e"
    NUM_RECORDS = 65536
    RECORD_SIZE = 1024
    ARCHIVE_MARKER = "[ARCHIVE_PUBLISH]"
    CLEANUP_MARKER = "[ARCHIVE_CLEANUP_COMMIT]"

    def __init__(self, test_context):
        super(ArchiveFetchE2ETest, self).__init__(test_context)
        self.kafka = None

    def _create_kafka(self):
        # Keep the WAL cache below the test data size so the fetch exercises S3-backed data.
        server_prop_overrides = [
            ["autobalancer.controller.enable", "false"],
            ["log.retention.check.interval.ms", "5000"],
            ["s3.wal.path", S3_WAL],
            ["s3.wal.cache.size", str(16 * 1024 * 1024)],
            ["s3.wal.capacity", str(64 * 1024 * 1024)],
            ["s3.wal.upload.threshold", str(1 * 1024 * 1024)],
            ["s3.stream.object.split.size", "1"],
            ["s3.stream.set.object.compaction.interval.minutes", "1"],
        ]
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=1,
            zk=None,
            version=DEV_BRANCH,
            kafka_heap_opts="-Xmx2048m -Xms2048m",
            server_prop_overrides=server_prop_overrides,
            extra_env=[
                "AUTOMQ_STREAM_ARCHIVE_COMPOSITE_TARGET_SIZE=1048576",
                "AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL=5000",
                "AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL=600000",
                "AUTOMQ_STREAM_OBJECT_COMPACTION_INTERVAL=5000",
                "AUTOMQ_STREAM_COMPACTION_MAJOR_V1_MAX_OBJECT_THRESHOLD=10",
                "AUTOMQ_STREAM_COMPACTION_COOLDOWN_AFTER_OPEN_STREAM=0",
                "AUTOMQ_STREAM_OBJECT_COMPACTION_JITTER_MAX_DELAY=1000",
            ],
            topics={
                self.TOPIC: {
                    "partitions": 1,
                    "replication-factor": 1,
                    "configs": {
                        "min.insync.replicas": 1,
                        "segment.bytes": 1048576,
                        "segment.ms": 5000,
                        "retention.bytes": -1,
                        "retention.ms": -1,
                    },
                }
            },
        )

    def _archive_published(self):
        lines = self.kafka.nodes[0].account.ssh_capture(
            "grep -F '%s' %s" % (self.ARCHIVE_MARKER, self.kafka.STDOUT_STDERR_CAPTURE),
            allow_fail=True,
        )
        return any(self.ARCHIVE_MARKER in line for line in lines)

    @cluster(num_nodes=3)
    def test_archive_fetch_from_beginning(self):
        """
        Given enough data to create and archive a Composite object, when a consumer
        fetches from offset zero, then every produced record is returned successfully.
        """
        self._create_kafka()
        self.kafka.start()

        producer = ProducerPerformanceService(
            self.test_context,
            1,
            self.kafka,
            topic=self.TOPIC,
            num_records=self.NUM_RECORDS,
            record_size=self.RECORD_SIZE,
            throughput=-1,
            version=DEV_BRANCH,
            settings={"acks": 1, "compression.type": "none"},
        )
        producer.run()
        assert sum(result["records"] for result in producer.results) == self.NUM_RECORDS

        wait_until(
            self._archive_published,
            timeout_sec=180,
            backoff_sec=1,
            err_msg="Archive did not publish a Composite object",
        )

        consumer = ConsoleConsumer(
            self.test_context,
            1,
            self.kafka,
            self.TOPIC,
            from_beginning=True,
            consumer_timeout_ms=60000,
            version=DEV_BRANCH,
        )
        consumer.run()
        consumed = len(consumer.messages_consumed[1])
        assert consumed == self.NUM_RECORDS, (
            "expected %d records after archive, but consumed %d" % (self.NUM_RECORDS, consumed)
        )


    def _cleanup_published(self):
        lines = self.kafka.nodes[0].account.ssh_capture(
            "grep -F '%s' %s" % (self.CLEANUP_MARKER, self.kafka.STDOUT_STDERR_CAPTURE),
            allow_fail=True,
        )
        return any(self.CLEANUP_MARKER in line for line in lines)

    def _delete_records(self, offset):
        node = self.kafka.nodes[0]
        delete_records_file = "/tmp/%s-delete-records.json" % self.TOPIC
        payload = (
            '{"partitions":[{"topic":"%s","partition":0,"offset":%d}],"version":1}'
            % (self.TOPIC, offset)
        )
        delete_records_script = self.kafka.path.script("kafka-delete-records.sh", node)
        command = "printf '%%s' '%s' > %s && %s --bootstrap-server %s --offset-json-file %s" % (
            payload,
            delete_records_file,
            delete_records_script,
            self.kafka.bootstrap_servers(),
            delete_records_file,
        )
        node.account.ssh(command)

    @cluster(num_nodes=3)
    def test_archive_cleanup_deletes_expired_objects(self):
        """
        Given archived objects and no retention limit, when DeleteRecords advances the
        partition start offset past them, then cleanup deletes their archive keys.
        """
        self._create_kafka()
        self.kafka.start()

        num_records = self.NUM_RECORDS
        producer = ProducerPerformanceService(
            self.test_context,
            1,
            self.kafka,
            topic=self.TOPIC,
            num_records=num_records,
            record_size=self.RECORD_SIZE,
            throughput=-1,
            version=DEV_BRANCH,
            settings={"acks": 1, "compression.type": "none"},
        )
        producer.run()
        assert sum(result["records"] for result in producer.results) == num_records

        wait_until(
            self._archive_published,
            timeout_sec=300,
            backoff_sec=2,
            err_msg="Archive did not publish an object for cleanup",
        )
        self._delete_records(num_records)
        wait_until(
            self._cleanup_published,
            timeout_sec=180,
            backoff_sec=2,
            err_msg="Archive cleanup did not commit",
        )
