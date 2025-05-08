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

import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.deserializer.proto.schema.MessageDefinition;
import kafka.automq.table.transformer.AvroKafkaRecordConvert;
import kafka.automq.table.transformer.Converter;
import kafka.automq.table.transformer.KafkaRecordConvert;
import kafka.automq.table.transformer.ProtobufKafkaRecordConvert;
import kafka.automq.table.transformer.RegistrySchemaAvroConverter;
import kafka.automq.table.worker.IcebergTableManager;
import kafka.automq.table.worker.IcebergWriter;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.server.record.TableTopicSchemaType;

import com.automq.stream.utils.Systems;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FieldsPerf {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldsPerf.class.getName());
    private static final Map<String, byte[]> IN_MEMORY_FILES;
    private static final List<String> TASKS;

    private static final List<String> FORMAT_TYPES;
    private static final long RECORDS_COUNT = Systems.getEnvLong("RECORDS_COUNT", 10000000);
    private static final Map<String, List<FormatTypeSupplier>> TASK_SUPPLIERS = new HashMap<>();

    static {
        // Get static field of InMemoryFileIO by reflection
        try {
            Class<?> clazz = Class.forName("org.apache.iceberg.inmemory.InMemoryFileIO");
            java.lang.reflect.Field field = clazz.getDeclaredField("IN_MEMORY_FILES");
            field.setAccessible(true);
            //noinspection unchecked
            IN_MEMORY_FILES = (Map<String, byte[]>) field.get(null);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        String tasksStr = System.getenv("TASKS");
        if (StringUtils.isBlank(tasksStr)) {
            tasksStr = "boolean,int,long,double,timestamp,string,binary,nested,array";
        }
        TASKS = List.of(tasksStr.split(","));

        String formatTypesStr = System.getenv("FORMAT_TYPES");
        if (StringUtils.isBlank(formatTypesStr)) {
            formatTypesStr = "proto,avro";
        }
        FORMAT_TYPES = List.of(formatTypesStr.split(","));

        // https://iceberg.apache.org/spec/#schemas-and-data-types
        TASK_SUPPLIERS.put("boolean", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().booleanType().noDefault(),
                        () -> ThreadLocalRandom.current().nextBoolean());
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "bool",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), ThreadLocalRandom.current().nextBoolean());
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("int", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().intType().noDefault(),
                        () -> ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "int32",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("long", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().longType().noDefault(),
                        () -> ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "int64",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("double", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().doubleType().noDefault(),
                        () -> ThreadLocalRandom.current().nextDouble(Long.MAX_VALUE));
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "double",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), ThreadLocalRandom.current().nextDouble(Long.MAX_VALUE));
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("timestamp", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type(LogicalTypes.timestampMillis()
                            .addToSchema(
                                Schema.create(Schema.Type.LONG)
                            )
                        ).withDefault(0),
                        () -> ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "int64",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("string", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().stringType().noDefault(),
                        () -> RandomStringUtils.randomAlphabetic(32));
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "string",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), RandomStringUtils.randomAlphabetic(32));
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("binary", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().bytesType().noDefault(),
                        () -> {
                            byte[] bytes = new byte[32];
                            ThreadLocalRandom.current().nextBytes(bytes);
                            return ByteBuffer.wrap(bytes);
                        });
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> msgBuilder.addField(
                            "required",
                            "bytes",
                            name,
                            number,
                            null
                        ),
                        (msgBuilder, name) -> {
                            byte[] bytes = new byte[32];
                            ThreadLocalRandom.current().nextBytes(bytes);
                            msgBuilder.setField(msgBuilder.getDescriptorForType().findFieldByName(name), bytes);
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("nested", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Schema nestedSchema = SchemaBuilder.builder().record("nested").fields().name("nf1").type().booleanType().noDefault().endRecord();
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type(nestedSchema).noDefault(),
                        () -> {
                            GenericRecord record = new GenericData.Record(nestedSchema);
                            record.put("nf1", ThreadLocalRandom.current().nextBoolean());
                            return record;
                        });
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> {
                            MessageDefinition nested = MessageDefinition.newBuilder("NestedType" + name)
                                .addField("required", "bool", "nf1", 1, null)
                                .build();
                            msgBuilder.addMessageDefinition(nested);
                            msgBuilder.addField("required", "NestedType" + name, name, number, null);
                        },
                        (msgBuilder, name) -> {
                            Descriptors.FieldDescriptor fieldByName = msgBuilder.getDescriptorForType().findFieldByName(name);
                            Descriptors.Descriptor nested = msgBuilder.getDescriptorForType().findNestedTypeByName("NestedType" + name);
                            DynamicMessage built = DynamicMessage.newBuilder(nested)
                                .setField(nested.findFieldByName("nf1"), ThreadLocalRandom.current().nextBoolean()).build();

                            msgBuilder.setField(fieldByName, built);
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
        TASK_SUPPLIERS.put("array", List.of(
            new FormatTypeSupplier(
                "avro",
                () -> {
                    Pair<Schema, List<byte[]>> perfArgs = perfAvroArgs(
                        s -> s.type().array().items(Schema.create(Schema.Type.BOOLEAN)).noDefault(),
                        () -> List.of(ThreadLocalRandom.current().nextBoolean()));
                    return new TypeCost("avro", new AvroPerf(perfArgs.getLeft(), perfArgs.getRight()).run());
                }
            ),
            new FormatTypeSupplier(
                "proto",
                () -> {
                    Pair<Descriptors.Descriptor, List<byte[]>> result = perfProtobufArgs(
                        (msgBuilder, name, number) -> {
                            msgBuilder.addField("repeated", "bool", name, number, null);
                        },
                        (msgBuilder, name) -> {
                            msgBuilder.addRepeatedField(msgBuilder.getDescriptorForType().findFieldByName(name), ThreadLocalRandom.current().nextBoolean());
                        }
                    );
                    return new TypeCost("proto", new ProtoBufPerf(result.getLeft(), result.getRight()).run());
                }
            )
        ));
    }

    public static void main(String[] args) {
        Map<String, List<Pair<String, Long>>> typeCosts = Map.of(
            "avro", new ArrayList<>(),
            "proto", new ArrayList<>()
        );

        for (String task : TASKS) {
            List<FormatTypeSupplier> formatTypeSuppliers = TASK_SUPPLIERS.get(task);
            for (FormatTypeSupplier formatTypeSupplier : formatTypeSuppliers) {

                if (FORMAT_TYPES.contains(formatTypeSupplier.format)) {
                    TypeCost typeCost = formatTypeSupplier.supplier.get();
                    typeCosts.get(typeCost.type).add(Pair.of(task, typeCost.cost()));
                }
            }
        }
        for (Map.Entry<String, List<Pair<String, Long>>> entry : typeCosts.entrySet()) {
            LOGGER.info("type: {}", entry.getKey());
            LOGGER.info("task cost: {}", entry.getValue());
        }
    }

    static Pair<org.apache.avro.Schema, List<byte[]>> perfAvroArgs(
        Function<SchemaBuilder.FieldBuilder<Schema>, SchemaBuilder.FieldAssembler<Schema>> fieldTypeBuilder,
        Supplier<Object> valueSupplier
    ) {
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.builder().record("test").fields();
        int fieldCountPerRecord = 32;
        for (int i = 0; i < fieldCountPerRecord; i++) {
            fa = fieldTypeBuilder.apply(fa.name("f" + i));
        }
        org.apache.avro.Schema schema = fa.endRecord();

        int payloadsCount = 1000;
        List<byte[]> payloads = new ArrayList<>(payloadsCount);
        for (int i = 0; i < payloadsCount; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            for (int j = 0; j < fieldCountPerRecord; j++) {
                avroRecord.put("f" + j, valueSupplier.get());
            }
            payloads.add(createAvroValue(avroRecord));
        }
        return Pair.of(schema, payloads);
    }

    @FunctionalInterface
    public interface FieldTypeBuilder {
        void apply(MessageDefinition.Builder msgDefBuilder, String fieldName, int fieldNumber);
    }

    @FunctionalInterface
    public interface FieldValueBuilder {
        void apply(DynamicMessage.Builder msgDefBuilder, String fieldName);
    }

    static Pair<Descriptors.Descriptor, List<byte[]>> perfProtobufArgs(
        FieldTypeBuilder fieldTypeBuilder,
        FieldValueBuilder fieldValueBuilder) {
        int fieldCountPerRecord = 32;
        int payloadsCount = 1000;

        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setPackage("example");
        schemaBuilder.setName("DynamicTest.proto");

        MessageDefinition.Builder msgDefBuilder = MessageDefinition.newBuilder("TestMessage");

        for (int i = 0; i < fieldCountPerRecord; i++) {
            String fieldName = "f" + i;
            int fieldNumber = i + 1;
            fieldTypeBuilder.apply(msgDefBuilder, fieldName, fieldNumber);
        }

        schemaBuilder.addMessageDefinition(msgDefBuilder.build());
        DynamicSchema schema;
        try {
            schema = schemaBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Schema build failed", e);
        }

        Descriptors.Descriptor descriptor = schema.getMessageDescriptor("TestMessage");

        List<byte[]> payloads = new ArrayList<>(payloadsCount);
        for (int i = 0; i < payloadsCount; i++) {
            DynamicMessage.Builder msgBuilder = DynamicMessage.newBuilder(descriptor);
            for (int j = 0; j < fieldCountPerRecord; j++) {
                String fieldName = "f" + j;
                fieldValueBuilder.apply(msgBuilder, fieldName);
            }
            payloads.add(createProtoBufValue(msgBuilder.build()));
        }

        return Pair.of(descriptor, payloads);
    }

    static class AvroPerf extends AbstractPerf<Schema> {
        public AvroPerf(Schema schema, List<byte[]> payloads) {
            super(schema, payloads);
        }

        @Override
        KafkaRecordConvert<GenericRecord> getKafkaRecordConverter(Schema schema) {
            StaticAvroDeserializer deserializer = new StaticAvroDeserializer(schema);
            return new AvroKafkaRecordConvert(deserializer);
        }
    }


    static class ProtoBufPerf extends AbstractPerf<Descriptors.Descriptor> {
        public ProtoBufPerf(Descriptors.Descriptor descriptor, List<byte[]> payloads) {
            super(descriptor, payloads);
        }

        @Override
        KafkaRecordConvert<GenericRecord> getKafkaRecordConverter(Descriptors.Descriptor descriptor) {
            StaticProtobufDeserializer deserializer = new StaticProtobufDeserializer(descriptor);
            return new ProtobufKafkaRecordConvert(deserializer);
        }
    }


    abstract static class AbstractPerf<T> {
        final T schema;
        final List<byte[]> payloads;
        long timeCost;


        public AbstractPerf(T schema, List<byte[]> payloads) {
            this.schema = schema;
            this.payloads = payloads;
        }

        public long run() {
            try {
                // warm up
                run0(100000);
                long start = System.nanoTime();
                run0(RECORDS_COUNT);
                timeCost = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                return timeCost;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public long timeCost() {
            return timeCost;
        }

        private void run0(long count) throws IOException {
            TableIdentifier tableId = TableIdentifier.parse("test.d1");
            WorkerConfig config = new IWorkerConfig();
            IcebergWriter writer = null;
            int size = 0;
            for (int i = 0; i < count; i++) {
                if (writer == null) {
                    InMemoryCatalog catalog = new InMemoryCatalog();
                    catalog.initialize("test", ImmutableMap.of());
                    KafkaRecordConvert<GenericRecord> recordConvert = getKafkaRecordConverter(schema);
                    Converter converter = new RegistrySchemaAvroConverter(recordConvert, "");
                    writer = new IcebergWriter(new IcebergTableManager(catalog, tableId, config), converter, config);
                    writer.setOffset(0, i);
                }
                byte[] payload = payloads.get(i % payloads.size());
                size += payload.length;

                writer.write(0, new SimpleRecord(i, payload));

                if (size > 32 * 1024 * 1024) {
                    size = 0;
                    writer.complete();
                    IN_MEMORY_FILES.clear();
                    writer = null;
                }
            }
            writer.complete();
        }

        abstract KafkaRecordConvert<GenericRecord> getKafkaRecordConverter(T schema);

    }


    static byte[] createAvroValue(GenericRecord record) {
        try {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(record, encoder);
            encoder.flush();
            byte[] avroBytes = outputStream.toByteArray();

            ByteBuf buf = Unpooled.buffer(1 + 4 + avroBytes.length);
            buf.writeByte((byte) 0x0);
            buf.writeInt(0);
            buf.writeBytes(avroBytes);

            return buf.array();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] createProtoBufValue(Message message) {
        try {
            byte[] protobufBytes = message.toByteArray();

            ByteBuf buf = Unpooled.buffer(1 + 4 + protobufBytes.length);
            buf.writeByte((byte) 0x0);
            buf.writeInt(0);
            buf.writeBytes(protobufBytes);

            return buf.array();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    static class StaticAvroDeserializer implements Deserializer<Object> {
        final org.apache.avro.Schema schema;
        final DatumReader<GenericRecord> reader;
        final DecoderFactory decoderFactory = DecoderFactory.get();

        public StaticAvroDeserializer(org.apache.avro.Schema schema) {
            this.schema = schema;
            this.reader = new GenericDatumReader<>(schema);
        }

        public Object deserialize(String topic, byte[] data) {
            try {
                return this.reader.read(null, decoderFactory.binaryDecoder(data, 5, data.length - 5, null));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class StaticProtobufDeserializer implements Deserializer<Message> {
        final Descriptors.Descriptor descriptor;

        public StaticProtobufDeserializer(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public Message deserialize(String s, byte[] bytes) {
            try {
                return DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(bytes, 5, bytes.length - 5));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class IWorkerConfig extends WorkerConfig {

        public IWorkerConfig() {
            super();
        }

        public String namespace() {
            return "test";
        }

        public TableTopicSchemaType schemaType() {
            return TableTopicSchemaType.SCHEMA;
        }

        public List<String> idColumns() {
            return Collections.emptyList();
        }

        public String partitionByConfig() {
            return null;
        }

        public List<String> partitionBy() {
            return Collections.emptyList();
        }

        public boolean upsertEnable() {
            return false;
        }

        public String cdcField() {
            return null;
        }
    }

    static class SimpleRecord implements org.apache.kafka.common.record.Record {
        final long offset;
        final byte[] value;

        public SimpleRecord(long offset, byte[] value) {
            this.offset = offset;
            this.value = value;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public int sequence() {
            return 0;
        }

        @Override
        public int sizeInBytes() {
            return value.length;
        }

        @Override
        public long timestamp() {
            return 0;
        }

        @Override
        public void ensureValid() {

        }

        @Override
        public int keySize() {
            return 0;
        }

        @Override
        public boolean hasKey() {
            return false;
        }

        @Override
        public ByteBuffer key() {
            return null;
        }

        @Override
        public int valueSize() {
            return 0;
        }

        @Override
        public boolean hasValue() {
            return true;
        }

        @Override
        public ByteBuffer value() {
            return ByteBuffer.wrap(value);
        }

        @Override
        public boolean hasMagic(byte b) {
            return false;
        }

        @Override
        public boolean isCompressed() {
            return false;
        }

        @Override
        public boolean hasTimestampType(TimestampType type) {
            return false;
        }

        @Override
        public Header[] headers() {
            return new Header[0];
        }
    }

    static final class TypeCost {
        private final String type;
        private final long cost;

        TypeCost(String type, long cost) {
            this.type = type;
            this.cost = cost;
        }

        public String type() {
            return type;
        }

        public long cost() {
            return cost;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (TypeCost) obj;
            return Objects.equals(this.type, that.type) &&
                this.cost == that.cost;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, cost);
        }

        @Override
        public String toString() {
            return "TypeCost[" +
                "type=" + type + ", " +
                "cost=" + cost + ']';
        }

    }

    static final class FormatTypeSupplier {
        private final String format;
        private final Supplier<TypeCost> supplier;

        FormatTypeSupplier(String format, Supplier<TypeCost> supplier) {
            this.format = format;
            this.supplier = supplier;
        }

        public String format() {
            return format;
        }

        public Supplier<TypeCost> supplier() {
            return supplier;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (FormatTypeSupplier) obj;
            return Objects.equals(this.format, that.format) &&
                Objects.equals(this.supplier, that.supplier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(format, supplier);
        }

        @Override
        public String toString() {
            return "FormatTypeSupplier[" +
                "format=" + format + ", " +
                "supplier=" + supplier + ']';
        }

    }

}
