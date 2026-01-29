package kafka.automq.zerozone;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LegacyRecordConverterTest {

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testMaybeConvertData")
    public void testMaybeConvert(String name, List<String> keys, List<String> values, byte magic, CompressionType compressionType,
        TimestampType timestampType) {
        testMaybeConvert0(keys, values, magic, compressionType, timestampType, 10);
    }

    public static Stream<Arguments> testMaybeConvertData() {
        return Stream.of(
            Arguments.of(
                "MagicV0 + Single Record",
                List.of("key1"),
                List.of("value1"),
                RecordBatch.MAGIC_VALUE_V0,
                CompressionType.NONE,
                TimestampType.NO_TIMESTAMP_TYPE
            ),
            Arguments.of(
                "MagicV0 + Multi Record",
                List.of("key1", "key2"),
                List.of("value1", "value2"),
                RecordBatch.MAGIC_VALUE_V0,
                CompressionType.NONE,
                TimestampType.NO_TIMESTAMP_TYPE
            ),
            Arguments.of(
                "MagicV0 + Multi Record + Compaction",
                List.of("key1", "key2"),
                List.of("value1", "value2"),
                RecordBatch.MAGIC_VALUE_V0,
                CompressionType.GZIP,
                TimestampType.NO_TIMESTAMP_TYPE
            ),
            Arguments.of(
                "MagicV1 + Single Record",
                List.of("key1"),
                List.of("value1"),
                RecordBatch.MAGIC_VALUE_V1,
                CompressionType.NONE,
                TimestampType.CREATE_TIME
            ),
            Arguments.of(
                "MagicV1 + Multi Record",
                List.of("key1", "key2"),
                List.of("value1", "value2"),
                RecordBatch.MAGIC_VALUE_V1,
                CompressionType.NONE,
                TimestampType.CREATE_TIME
            ),
            Arguments.of(
                "MagicV1 + Multi Record + Compaction",
                List.of("key1", "key2"),
                List.of("value1", "value2"),
                RecordBatch.MAGIC_VALUE_V1,
                CompressionType.GZIP,
                TimestampType.CREATE_TIME
            )
        );
    }


    void testMaybeConvert0(List<String> keys, List<String> values, byte magic, CompressionType compressionType,
        TimestampType timestampType, long baseTimestamp) {
        MemoryRecords fromRecords = records(keys, values, magic, compressionType, timestampType, baseTimestamp);
        MemoryRecords toRecords = LegacyRecordConverter.maybeConvert(fromRecords);
        Iterator<Record> it = toRecords.records().iterator();
        for (int i = 0; i < values.size(); i++) {
            Record record = it.next();
            assertEquals(string2ByteBuffer(keys.get(i)), record.key());
            assertEquals(string2ByteBuffer(values.get(i)), record.value());
            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(baseTimestamp + i, record.timestamp());
            }
        }
    }

    private MemoryRecords records(List<String> keys, List<String> values, byte magic, CompressionType compressionType,
        TimestampType timestampType, long baseTimestamp) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1), magic, Compression.of(compressionType).build(), timestampType, 0L);
        for (int i = 0; i < values.size(); i++) {
            builder.append(baseTimestamp + i, string2ByteBuffer(keys.get(i)), string2ByteBuffer(values.get(i)));
        }
        return builder.build();
    }

    private ByteBuffer string2ByteBuffer(String s) {
        return s == null ? null : ByteBuffer.wrap(s.getBytes());
    }
}
