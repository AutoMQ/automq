/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.automq.table.perf;

import kafka.automq.table.process.Converter;
import kafka.automq.table.process.convert.AvroRegistryConverter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Runs a suite of single-field conversion benchmarks to compare end-to-end CPU cost per field type/shape.
 */
public final class FieldProfileRunner {

    private static final List<FieldScenario> DEFAULT_SCENARIOS = List.of(
        FieldScenario.primitive("boolean", DataType.BOOLEAN, Schema.create(Schema.Type.BOOLEAN)),
        FieldScenario.primitive("int", DataType.INT, Schema.create(Schema.Type.INT)),
        FieldScenario.primitive("long", DataType.LONG, Schema.create(Schema.Type.LONG)),
        FieldScenario.primitive("double", DataType.DOUBLE, Schema.create(Schema.Type.DOUBLE)),
        FieldScenario.logical("timestamp-millis", DataType.TIMESTAMP,
            LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))),
        FieldScenario.string("string-8", 8),
        FieldScenario.string("string-32", 32),
        FieldScenario.string("string-128", 128),
        FieldScenario.string("string-256", 256),
        FieldScenario.string("string-512", 512),
        FieldScenario.string("string-2048", 2048),
        FieldScenario.string("string-4096", 4096),
        FieldScenario.binary("binary-8", 8),
        FieldScenario.binary("binary-32", 32),
        FieldScenario.binary("binary-128", 128),
        FieldScenario.binary("binary-256", 256),
        FieldScenario.binary("binary-512", 512),
        FieldScenario.binary("binary-2048", 2048),
        FieldScenario.fixed("fixed-16", 16),
        FieldScenario.fixed("fixed-128", 128),
        FieldScenario.decimal("decimal-38-4", 38, 4),
        FieldScenario.uuid("uuid"),
        FieldScenario.list("list-int-16", Schema.create(Schema.Type.INT), 16),
        FieldScenario.map("map-string-long-16", Schema.create(Schema.Type.LONG), 16),
        FieldScenario.struct("struct-nested", List.of(
            Schema.create(Schema.Type.INT),
            Schema.create(Schema.Type.LONG),
            Schema.create(Schema.Type.STRING)))
    );

    private FieldProfileRunner() {
    }

    public static void main(String[] args) {
        ProfileConfig config = ProfileConfig.fromEnv();
        List<FieldScenario> scenarios = config.resolvedScenarios(DEFAULT_SCENARIOS);
        Map<String, BenchmarkResult> results = new LinkedHashMap<>();

        System.out.printf("Field profile records=%d payloads=%d batchBytes=%d rounds=%d%n",
            config.recordsCount, config.payloadCount, config.batchSizeBytes, config.rounds);

        for (FieldScenario scenario : scenarios) {
            SingleFieldAvroTestCase testCase = new SingleFieldAvroTestCase(
                scenario,
                config.recordsCount,
                config.payloadCount,
                config.batchSizeBytes);

            System.out.printf("Scenario: %s (%s)%n", scenario.name, scenario.describe());

            long totalDurationNs = 0L;
            long totalFieldCount = 0L;
            long totalRecords = 0L;
            boolean success = true;

            for (int round = 1; round <= config.rounds; round++) {
                BenchmarkResult result = testCase.runBenchmark(config.recordsCount);
                if (!result.isSuccess()) {
                    System.out.printf("  round %d failed: %s%n", round, result.getErrorMessage());
                    success = false;
                    break;
                }

                totalDurationNs += result.getDurationNs();
                totalFieldCount += result.getFieldCount();
                totalRecords += result.getRecordsProcessed();

                System.out.printf(Locale.ROOT,
                    "  round %d: %d ms, fieldCount=%d, ns/field=%.2f, ns/record=%.2f%n",
                    round,
                    result.getDurationMs(),
                    result.getFieldCount(),
                    result.getNsPerField(),
                    result.getNsPerRecord());
            }

            if (success && totalRecords > 0) {
                BenchmarkResult aggregated = BenchmarkResult.success(
                    testCase.formatName,
                    scenario.name,
                    totalDurationNs,
                    totalRecords,
                    totalFieldCount);
                results.put(scenario.name, aggregated);
                System.out.printf(Locale.ROOT,
                    "  aggregated: ns/field=%.2f, ns/record=%.2f (over %d rounds)%n",
                    aggregated.getNsPerField(),
                    aggregated.getNsPerRecord(),
                    config.rounds);
            } else {
                results.put(scenario.name, BenchmarkResult.failure(testCase.formatName, scenario.name,
                    "round failed"));
            }
        }

        summarize(results);
    }

    private static void summarize(Map<String, BenchmarkResult> results) {
        BenchmarkResult primitiveBaseline = results.get("int");
        double baselineNs = primitiveBaseline != null ? primitiveBaseline.getNsPerField() : 0.0d;

        System.out.println();
        System.out.println("=== Summary ===");
        System.out.printf(Locale.ROOT, "%-24s %-12s %-12s %-12s %-12s%n",
            "scenario", "ns/field", "ns/record", "fieldCount", "rel");

        results.forEach((name, result) -> {
            if (!result.isSuccess()) {
                System.out.printf("%-24s %-12s %-12s %-12s %-12s%n",
                    name, "FAIL", "-", "-", "-");
                return;
            }
            double rel = baselineNs == 0.0d ? 0.0d : result.getNsPerField() / baselineNs;
            System.out.printf(Locale.ROOT, "%-24s %-12.2f %-12.2f %-12d %-12.2f%n",
                name,
                result.getNsPerField(),
                result.getNsPerRecord(),
                result.getFieldCount(),
                rel);
        });
    }

    private static final class ProfileConfig {
        final long recordsCount;
        final int payloadCount;
        final int batchSizeBytes;
        final Set<String> scenarioFilter;
        final int rounds;

        private ProfileConfig(long recordsCount, int payloadCount, int batchSizeBytes, Set<String> scenarioFilter, int rounds) {
            this.recordsCount = recordsCount;
            this.payloadCount = payloadCount;
            this.batchSizeBytes = batchSizeBytes;
            this.scenarioFilter = scenarioFilter;
            this.rounds = rounds;
        }

        static ProfileConfig fromEnv() {
            long records = parseLongEnv("FIELD_PROFILE_RECORDS", 2_000_000L);
            int payloads = (int) parseLongEnv("FIELD_PROFILE_PAYLOADS", 1_024L);
            int batchSize = (int) parseLongEnv("FIELD_PROFILE_BATCH_BYTES", 32 * 1024 * 1024L);
            Set<String> filter = EnvParsers.parseCsvToSet(System.getenv("FIELD_PROFILE_SCENARIOS"));
            int rounds = (int) parseLongEnv("FIELD_PROFILE_ROUNDS", 3L);
            return new ProfileConfig(records, payloads, batchSize, filter, Math.max(rounds, 1));
        }

        List<FieldScenario> resolvedScenarios(List<FieldScenario> defaults) {
            if (scenarioFilter == null || scenarioFilter.isEmpty()) {
                return defaults;
            }
            List<FieldScenario> filtered = new ArrayList<>();
            for (FieldScenario scenario : defaults) {
                if (scenarioFilter.contains(scenario.name)) {
                    filtered.add(scenario);
                }
            }
            return filtered.isEmpty() ? defaults : filtered;
        }

        private static long parseLongEnv(String key, long defaultValue) {
            String value = System.getenv(key);
            if (value == null || value.isEmpty()) {
                return defaultValue;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }

    private static final class SingleFieldAvroTestCase extends PerfTestCase {
        private final FieldScenario scenario;
        private final Schema recordSchema;
        private final PayloadManager payloadManager;
        private final StaticAvroDeserializer deserializer;

        SingleFieldAvroTestCase(FieldScenario scenario, long recordsCount, int payloadCount, int batchSizeBytes) {
            super(scenario.baseDataType, "avro", scenario.fieldCount, payloadCount,
                new InlinePerfConfig(recordsCount, scenario.fieldCount, payloadCount, batchSizeBytes));
            this.scenario = scenario;
            this.recordSchema = scenario.buildRecordSchema();
            this.deserializer = new StaticAvroDeserializer(recordSchema);
            this.payloadManager = new PayloadManager(this::generatePayload, payloadCount);
        }

        @Override
        PayloadManager getPayloadManager() {
            return payloadManager;
        }

        @Override
        protected byte[] generatePayload() {
            GenericRecord record = new GenericData.Record(recordSchema);
            scenario.populate(record);
            return encodeWithHeader(recordSchema, record);
        }

        @Override
        protected Converter createConverter() {
            return new AvroRegistryConverter(deserializer, null);
        }

        private static byte[] encodeWithHeader(Schema schema, GenericRecord record) {
            try {
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                datumWriter.write(record, encoder);
                encoder.flush();
                byte[] avroBytes = outputStream.toByteArray();

                ByteBuf buffer = Unpooled.buffer(1 + 4 + avroBytes.length);
                buffer.writeByte((byte) 0x0);
                buffer.writeInt(0);
                buffer.writeBytes(avroBytes);
                return buffer.array();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class InlinePerfConfig extends PerfConfig {
        private final long recordsCount;
        private final int fieldCount;
        private final int payloadsCount;
        private final int batchSizeBytes;

        InlinePerfConfig(long recordsCount, int fieldCount, int payloadsCount, int batchSizeBytes) {
            this.recordsCount = recordsCount;
            this.fieldCount = fieldCount;
            this.payloadsCount = payloadsCount;
            this.batchSizeBytes = batchSizeBytes;
        }

        @Override
        public long getRecordsCount() {
            return recordsCount;
        }

        @Override
        public int getFieldCountPerRecord() {
            return fieldCount;
        }

        @Override
        public int getPayloadsCount() {
            return payloadsCount;
        }

        @Override
        public int getBatchSizeBytes() {
            return batchSizeBytes;
        }
    }

    private static final class FieldScenario {
        final String name;
        final DataType baseDataType;
        final int fieldCount;
        final Schema fieldSchema;
        final ScenarioKind kind;
        final int size;
        final int decimalPrecision;
        final int decimalScale;
        final List<Schema> structFieldSchemas;

        private FieldScenario(String name, DataType baseDataType, int fieldCount, Schema fieldSchema, ScenarioKind kind, int size) {
            this(name, baseDataType, fieldCount, fieldSchema, kind, size, 0, 0, List.of());
        }

        private FieldScenario(String name, DataType baseDataType, int fieldCount, Schema fieldSchema, ScenarioKind kind,
                              int size, int decimalPrecision, int decimalScale, List<Schema> structFieldSchemas) {
            this.name = name;
            this.baseDataType = baseDataType;
            this.fieldCount = fieldCount;
            this.fieldSchema = fieldSchema;
            this.kind = kind;
            this.size = size;
            this.decimalPrecision = decimalPrecision;
            this.decimalScale = decimalScale;
            this.structFieldSchemas = structFieldSchemas;
        }

        static FieldScenario primitive(String name, DataType dataType, Schema schema) {
            return new FieldScenario(name, dataType, 1, schema, ScenarioKind.PRIMITIVE, 0);
        }

        static FieldScenario logical(String name, DataType dataType, Schema schema) {
            return new FieldScenario(name, dataType, 1, schema, ScenarioKind.PRIMITIVE, 0);
        }

        static FieldScenario string(String name, int length) {
            return new FieldScenario(name, DataType.STRING, 1, Schema.create(Schema.Type.STRING), ScenarioKind.STRING, length);
        }

        static FieldScenario binary(String name, int length) {
            return new FieldScenario(name, DataType.BINARY, 1, Schema.create(Schema.Type.BYTES), ScenarioKind.BINARY, length);
        }

        static FieldScenario fixed(String name, int length) {
            String fixedName = name.replace('-', '_') + "_fixed";
            return new FieldScenario(name, DataType.BINARY, 1, Schema.createFixed(fixedName, null, null, length), ScenarioKind.FIXED, length);
        }

        static FieldScenario list(String name, Schema elementSchema, int elementCount) {
            Schema listSchema = Schema.createArray(elementSchema);
            return new FieldScenario(name, DataType.ARRAY, 1, listSchema, ScenarioKind.LIST, elementCount);
        }

        static FieldScenario map(String name, Schema valueSchema, int entryCount) {
            Schema mapSchema = Schema.createMap(valueSchema);
            return new FieldScenario(name, DataType.NESTED, 1, mapSchema, ScenarioKind.MAP, entryCount);
        }

        static FieldScenario decimal(String name, int precision, int scale) {
            Schema decimalSchema = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
            return new FieldScenario(name, DataType.BINARY, 1, decimalSchema, ScenarioKind.DECIMAL, 0, precision, scale, List.of());
        }

        static FieldScenario uuid(String name) {
            Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
            return new FieldScenario(name, DataType.STRING, 1, uuidSchema, ScenarioKind.UUID, 0);
        }

        static FieldScenario struct(String name, List<Schema> fieldSchemas) {
            SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record(name.replace('-', '_') + "_struct")
                .fields();
            for (int i = 0; i < fieldSchemas.size(); i++) {
                assembler.name("sf" + i).type(fieldSchemas.get(i)).noDefault();
            }
            Schema structSchema = assembler.endRecord();
            return new FieldScenario(name, DataType.NESTED, 1, structSchema, ScenarioKind.STRUCT, fieldSchemas.size(), 0, 0, fieldSchemas);
        }

        Schema buildRecordSchema() {
            SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record(name.replace('-', '_'))
                .namespace("kafka.automq.table.perf")
                .fields();
            assembler.name("f0").type(fieldSchema).noDefault();
            return assembler.endRecord();
        }

        void populate(GenericRecord record) {
            record.put("f0", value());
        }

        Object value() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            return switch (kind) {
                case PRIMITIVE -> primitiveValue(random);
                case STRING -> new org.apache.avro.util.Utf8(randomString(size));
                case BINARY -> ByteBuffer.wrap(randomBytes(size));
                case FIXED -> {
                    GenericData.Fixed fixed = new GenericData.Fixed(fieldSchema);
                    fixed.bytes(randomBytes(size));
                    yield fixed;
                }
                case DECIMAL -> decimalValue();
                case UUID -> new org.apache.avro.util.Utf8(UUID.randomUUID().toString());
                case LIST -> {
                    List<Object> list = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        list.add(ThreadLocalRandom.current().nextInt(2) == 0 ? 0 : 1);
                    }
                    yield list;
                }
                case MAP -> {
                    Map<String, Object> map = new LinkedHashMap<>(size);
                    for (int i = 0; i < size; i++) {
                        map.put("k" + i, random.nextLong());
                    }
                    yield map;
                }
                case STRUCT -> {
                    GenericRecord nested = new GenericData.Record(fieldSchema);
                    for (Schema.Field field : fieldSchema.getFields()) {
                        nested.put(field.name(), randomValueForSchema(field.schema()));
                    }
                    yield nested;
                }
            };
        }

        String describe() {
            return switch (kind) {
                case PRIMITIVE -> fieldSchema.getType().getName();
                case STRING -> "string(len=" + size + ")";
                case BINARY -> "bytes(len=" + size + ")";
                case FIXED -> "fixed(len=" + size + ")";
                case DECIMAL -> "decimal(precision=" + decimalPrecision + ",scale=" + decimalScale + ")";
                case UUID -> "uuid";
                case LIST -> "list[size=" + size + "]";
                case MAP -> "map[size=" + size + "]";
                case STRUCT -> "struct(fields=" + size + ")";
            };
        }

        private Object primitiveValue(ThreadLocalRandom random) {
            return switch (fieldSchema.getType()) {
                case BOOLEAN -> random.nextBoolean();
                case INT -> random.nextInt();
                case LONG -> random.nextLong();
                case DOUBLE -> random.nextDouble();
                default -> random.nextLong();
            };
        }

        private static byte[] randomBytes(int size) {
            byte[] bytes = new byte[size];
            ThreadLocalRandom.current().nextBytes(bytes);
            return bytes;
        }

        private static String randomString(int size) {
            return RandomStringUtils.randomAlphanumeric(size);
        }

        private Object decimalValue() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            BigDecimal value = BigDecimal.valueOf(random.nextDouble())
                .multiply(BigDecimal.TEN.pow(decimalPrecision - Math.max(decimalScale, 1)))
                .setScale(decimalScale, RoundingMode.HALF_UP);
            byte[] bytes = value.unscaledValue().toByteArray();
            return ByteBuffer.wrap(bytes);
        }

        private Object randomValueForSchema(Schema schema) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            return switch (schema.getType()) {
                case BOOLEAN -> random.nextBoolean();
                case INT -> random.nextInt();
                case LONG -> random.nextLong();
                case DOUBLE -> random.nextDouble();
                case STRING -> new org.apache.avro.util.Utf8(randomString(8));
                case BYTES -> ByteBuffer.wrap(randomBytes(16));
                default -> null;
            };
        }
    }

    private enum ScenarioKind {
        PRIMITIVE,
        STRING,
        BINARY,
        FIXED,
        DECIMAL,
        UUID,
        LIST,
        MAP,
        STRUCT
    }

    private static final class EnvParsers {
        private EnvParsers() {
        }

        static Set<String> parseCsvToSet(String csv) {
            if (csv == null || csv.isEmpty()) {
                return null;
            }
            String[] parts = csv.split(",");
            Set<String> set = new LinkedHashSet<>(parts.length);
            for (String part : parts) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    set.add(trimmed);
                }
            }
            return set;
        }
    }
}
