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
package kafka.automq.table.process.convert;

import kafka.automq.table.binder.RecordBinder;
import kafka.automq.table.deserializer.proto.CustomProtobufSchema;
import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;
import kafka.automq.table.deserializer.proto.parse.ProtobufSchemaParser;
import kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants;
import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.process.ConversionResult;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.TaskWriter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

import static kafka.automq.table.binder.AvroRecordBinderTypeTest.createTableWriter;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("S3Unit")
public class ProtobufRegistryConverterUnitTest {

    private static final String BASIC_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        message BasicRecord {
            bool active = 1;
            int32 score = 2;
            uint32 quota = 3;
            int64 total = 4;
            uint64 big_total = 5;
            float ratio = 6;
            double precise = 7;
            string name = 8;
            bytes payload = 9;
            Status status = 10;
            Nested meta = 11;
        }

        message Nested {
            string note = 1;
        }

        enum Status {
            STATUS_UNSPECIFIED = 0;
            STATUS_READY = 1;
        }
        """;

    private static final String COLLECTION_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        message CollectionRecord {
            repeated string tags = 1;
            repeated Item notes = 2;
            repeated Wrap wrappers = 3;
            map<string, int32> counters = 4;
            map<int32, Item> keyed_items = 5;
            map<string, Wrap> wrap_map = 6;
        }

        message Item {
            string value = 1;
        }

        message Wrap {
            Item item = 1;
            repeated Item items = 2;
        }
        """;

    private static final String OPTIONAL_COLLECTION_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        import \"google/protobuf/timestamp.proto\";

        message OptionalCollectionRecord {
            optional Wrapper opt_wrapper = 1;
            optional IntStringMap opt_int_map = 2;
            optional Item opt_item = 3;
            optional WrapMapHolder opt_wrap_map = 4;
            optional google.protobuf.Timestamp opt_ts = 5;
        }

        message Item {
            string value = 1;
        }

        message Wrapper {
            repeated Item items = 1;
        }

        message WrapMapHolder {
            map<string, Wrapper> entries = 1;
        }

        message IntStringMap {
            map<int32, string> entries = 1;
        }
        """;

    private static final String ADVANCED_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        import \"google/protobuf/timestamp.proto\";

        message AdvancedRecord {
            optional string opt_str = 1;
            optional int32 opt_int = 2;
            optional Ref opt_ref = 3;

            oneof selection {
                string selection_str = 4;
                int32 selection_int = 5;
                Ref selection_ref = 6;
                Bag selection_bag = 7;
                MapHolder selection_map = 8;
                IntMapHolder selection_int_map = 9;
            }

            google.protobuf.Timestamp event_time = 10;
            Ref direct = 11;
            repeated Ref refs = 12;
        }

        message Ref {
            string name = 1;
        }

        message Bag {
            repeated Ref refs = 1;
        }

        message MapHolder {
            map<string, int32> entries = 1;
        }

        message IntMapHolder {
            map<int32, string> entries = 1;
        }
        """;

    private static final String RECURSIVE_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        message Node {
            string id = 1;
            Child child = 2;
        }

        message Child {
            Node leaf = 1;
        }
        """;

    @Test
    void convertBasicTypesRecord() throws Exception {
        String topic = "proto-basic";
        ConversionResult result = convert(topic, BASIC_PROTO, "BasicRecord", builder -> {
            builder.setField(builder.getDescriptorForType().findFieldByName("active"), true);
            builder.setField(builder.getDescriptorForType().findFieldByName("score"), -10);
            builder.setField(builder.getDescriptorForType().findFieldByName("quota"), -1); // uint32 max
            builder.setField(builder.getDescriptorForType().findFieldByName("total"), -123456789L);
            builder.setField(builder.getDescriptorForType().findFieldByName("big_total"), -1L);
            builder.setField(builder.getDescriptorForType().findFieldByName("ratio"), 1.5f);
            builder.setField(builder.getDescriptorForType().findFieldByName("precise"), 3.14159d);
            builder.setField(builder.getDescriptorForType().findFieldByName("name"), "basic-name");
            builder.setField(
                builder.getDescriptorForType().findFieldByName("payload"),
                ByteString.copyFromUtf8("payload-bytes")
            );
            builder.setField(
                builder.getDescriptorForType().findFieldByName("status"),
                builder.getDescriptorForType().getFile().findEnumTypeByName("Status").findValueByName("STATUS_READY")
            );
            Descriptors.FieldDescriptor nestedField = builder.getDescriptorForType().findFieldByName("meta");
            builder.setField(nestedField, nestedMessage(nestedField.getMessageType(), "note-value"));
        });

        GenericRecord record = (GenericRecord) result.getValue();
        assertEquals(true, record.get("active"));
        assertEquals(-10, record.get("score"));
        int quotaSigned = (Integer) record.get("quota");
        assertEquals("4294967295", Long.toUnsignedString(Integer.toUnsignedLong(quotaSigned)));
        assertEquals(-123456789L, record.get("total"));
        long bigTotal = (Long) record.get("big_total");
        assertEquals("18446744073709551615", Long.toUnsignedString(bigTotal));
        assertEquals(1.5f, (Float) record.get("ratio"), 1e-6);
        assertEquals(3.14159d, (Double) record.get("precise"), 1e-9);
        assertEquals("basic-name", record.get("name").toString());
        assertEquals("payload-bytes", utf8(record.get("payload")));
        assertEquals("STATUS_READY", record.get("status").toString());
        assertEquals("note-value", ((GenericRecord) record.get("meta")).get("note").toString());

        bindAndWrite(record);
    }

    @Test
    void convertCollectionsRecord() throws Exception {
        String topic = "proto-collections";
        ConversionResult result = convert(topic, COLLECTION_PROTO, "CollectionRecord", builder -> {
            Descriptors.FieldDescriptor tagsFd = builder.getDescriptorForType().findFieldByName("tags");
            builder.addRepeatedField(tagsFd, "alpha");
            builder.addRepeatedField(tagsFd, "beta");

            Descriptors.FieldDescriptor notesFd = builder.getDescriptorForType().findFieldByName("notes");
            Descriptors.Descriptor itemDesc = notesFd.getMessageType();
            builder.addRepeatedField(notesFd, nestedMessage(itemDesc, "note-1"));
            builder.addRepeatedField(notesFd, nestedMessage(itemDesc, "note-2"));

            Descriptors.FieldDescriptor wrappersFd = builder.getDescriptorForType().findFieldByName("wrappers");
            Descriptors.Descriptor wrapDesc = wrappersFd.getMessageType();
            Descriptors.Descriptor wrapItemDesc = wrapDesc.findFieldByName("item").getMessageType();
            builder.addRepeatedField(wrappersFd, wrapMessage(wrapDesc, wrapItemDesc, "w1-a", List.of("w1-b")));
            builder.addRepeatedField(wrappersFd, wrapMessage(wrapDesc, wrapItemDesc, "w2-a", List.of("w2-b", "w2-c")));

            Descriptors.FieldDescriptor countersFd = builder.getDescriptorForType().findFieldByName("counters");
            Descriptors.Descriptor countersEntry = countersFd.getMessageType();
            builder.addRepeatedField(countersFd, mapEntry(countersEntry, "k1", 10));
            builder.addRepeatedField(countersFd, mapEntry(countersEntry, "k2", 20));

            Descriptors.FieldDescriptor keyedFd = builder.getDescriptorForType().findFieldByName("keyed_items");
            Descriptors.Descriptor keyedEntry = keyedFd.getMessageType();
            builder.addRepeatedField(keyedFd, mapEntry(keyedEntry, 1, nestedMessage(keyedEntry.findFieldByName("value").getMessageType(), "v1")));
            builder.addRepeatedField(keyedFd, mapEntry(keyedEntry, 2, nestedMessage(keyedEntry.findFieldByName("value").getMessageType(), "v2")));

            Descriptors.FieldDescriptor wrapMapFd = builder.getDescriptorForType().findFieldByName("wrap_map");
            Descriptors.Descriptor wrapMapEntry = wrapMapFd.getMessageType();
            Descriptors.Descriptor wrapMapValueDesc = wrapMapEntry.findFieldByName("value").getMessageType();
            Descriptors.Descriptor wrapMapItemDesc = wrapMapValueDesc.findFieldByName("item").getMessageType();
            builder.addRepeatedField(wrapMapFd, mapEntry(
                wrapMapEntry,
                "wm1",
                wrapMessage(wrapMapValueDesc, wrapMapItemDesc, "wm1-a", List.of("wm1-b"))
            ));
            builder.addRepeatedField(wrapMapFd, mapEntry(
                wrapMapEntry,
                "wm2",
                wrapMessage(wrapMapValueDesc, wrapMapItemDesc, "wm2-a", List.of("wm2-b", "wm2-c"))
            ));
        });

        GenericRecord record = (GenericRecord) result.getValue();
        List<?> tags = (List<?>) record.get("tags");
        assertEquals(List.of("alpha", "beta"), tags.stream().map(Object::toString).collect(Collectors.toList()));

        List<?> notes = (List<?>) record.get("notes");
        assertEquals(List.of("note-1", "note-2"), notes.stream()
            .map(GenericRecord.class::cast)
            .map(r -> r.get("value").toString())
            .collect(Collectors.toList()));

        Map<String, Integer> counters = logicalMapToMap(record.get("counters"));
        assertEquals(Map.of("k1", 10, "k2", 20), counters);

        Map<Integer, String> keyed = logicalMapToMap(record.get("keyed_items"));
        assertEquals(Map.of(1, "v1", 2, "v2"), keyed);

        List<?> wrappers = (List<?>) record.get("wrappers");
        assertEquals(List.of("w1-a", "w2-a"), wrappers.stream()
            .map(GenericRecord.class::cast)
            .map(r -> ((GenericRecord) r.get("item")).get("value").toString())
            .collect(Collectors.toList()));
        assertEquals(List.of(
            List.of("w1-b"),
            List.of("w2-b", "w2-c")
        ), wrappers.stream()
            .map(GenericRecord.class::cast)
            .map(r -> (List<?>) r.get("items"))
            .map(lst -> lst.stream()
                .map(GenericRecord.class::cast)
                .map(it -> it.get("value").toString())
                .collect(Collectors.toList()))
            .collect(Collectors.toList()));

        Map<String, Object> wrapMap = logicalMapToMap(record.get("wrap_map"));
        assertEquals("wm1-a", ((GenericRecord) ((GenericRecord) wrapMap.get("wm1")).get("item")).get("value").toString());
        assertEquals("wm2-a", ((GenericRecord) ((GenericRecord) wrapMap.get("wm2")).get("item")).get("value").toString());

        bindAndWrite(record);
    }

    @Test
    void convertAdvancedRecord() throws Exception {
        String topic = "proto-advanced";
        ConversionResult result = convert(topic, ADVANCED_PROTO, "AdvancedRecord", builder -> {
            builder.setField(builder.getDescriptorForType().findFieldByName("opt_str"), "optional-value");
            builder.setField(builder.getDescriptorForType().findFieldByName("opt_int"), 99);
            Descriptors.FieldDescriptor optRefFd = builder.getDescriptorForType().findFieldByName("opt_ref");
            builder.setField(optRefFd, nestedMessage(optRefFd.getMessageType(), "opt-ref"));

            // choose oneof map branch via MapHolder; other branches should remain null
            Descriptors.FieldDescriptor selMapFd = builder.getDescriptorForType().findFieldByName("selection_map");
            Descriptors.Descriptor mapHolderDesc = selMapFd.getMessageType();
            Descriptors.FieldDescriptor entriesFd = mapHolderDesc.findFieldByName("entries");
            Descriptors.Descriptor entryDesc = entriesFd.getMessageType();
            DynamicMessage.Builder holderBuilder = DynamicMessage.newBuilder(mapHolderDesc);
            holderBuilder.addRepeatedField(entriesFd, mapEntry(entryDesc, "a", 1));
            holderBuilder.addRepeatedField(entriesFd, mapEntry(entryDesc, "b", 2));
            builder.setField(selMapFd, holderBuilder.build());
            Timestamp timestamp = Timestamp.newBuilder().setSeconds(1234L).setNanos(567000000).build();
            builder.setField(builder.getDescriptorForType().findFieldByName("event_time"), timestamp);

            Descriptors.FieldDescriptor refField = builder.getDescriptorForType().findFieldByName("direct");
            builder.setField(refField, nestedMessage(refField.getMessageType(), "parent"));

            Descriptors.FieldDescriptor refsField = builder.getDescriptorForType().findFieldByName("refs");
            Descriptors.Descriptor refDescriptor = refsField.getMessageType();
            builder.addRepeatedField(refsField, nestedMessage(refDescriptor, "child-1"));
            builder.addRepeatedField(refsField, nestedMessage(refDescriptor, "child-2"));
        });

        GenericRecord record = (GenericRecord) result.getValue();
        Schema optionalSchema = record.getSchema().getField("opt_str").schema();
        assertEquals(Schema.Type.UNION, optionalSchema.getType());
        assertEquals(Schema.Type.STRING, optionalSchema.getTypes().get(0).getType());
        assertEquals("optional-value", record.get("opt_str").toString());
        assertEquals(99, record.get("opt_int"));
        assertEquals("opt-ref", ((GenericRecord) record.get("opt_ref")).get("name").toString());

        GenericRecord selMapRecord = (GenericRecord) record.get("selection_map");
        Map<String, Integer> selMap = logicalMapToMap(selMapRecord.get("entries"));
        assertEquals(Map.of("a", 1, "b", 2), selMap);
        assertEquals(null, record.get("selection_ref"));
        assertEquals(null, record.get("selection_str"));
        assertEquals(null, record.get("selection_int"));
        assertEquals(null, record.get("selection_bag"));

        long expectedMicros = 1234_000_000L + 567_000;
        assertEquals(expectedMicros, record.get("event_time"));
        assertEquals("parent", ((GenericRecord) record.get("direct")).get("name").toString());

        List<?> refs = (List<?>) record.get("refs");
        assertEquals(List.of("child-1", "child-2"), refs.stream()
            .map(GenericRecord.class::cast)
            .map(r -> r.get("name").toString())
            .collect(Collectors.toList()));

        bindAndWrite(record);
    }

    @Test
    void convertAdvancedOneofStringIntRefBag() throws Exception {
        // string branch
        ConversionResult stringResult = convert("proto-adv-oneof-str", ADVANCED_PROTO, "AdvancedRecord", b ->
            b.setField(b.getDescriptorForType().findFieldByName("selection_str"), "sel-str"));
        GenericRecord stringRec = (GenericRecord) stringResult.getValue();
        assertEquals("sel-str", stringRec.get("selection_str"));
        assertEquals(null, stringRec.get("selection_int"));
        assertEquals(null, stringRec.get("selection_ref"));
        assertEquals(null, stringRec.get("selection_bag"));
        bindAndWrite((GenericRecord) stringResult.getValue());

        // int branch
        ConversionResult intResult = convert("proto-adv-oneof-int", ADVANCED_PROTO, "AdvancedRecord", b ->
            b.setField(b.getDescriptorForType().findFieldByName("selection_int"), 123));
        GenericRecord intRec = (GenericRecord) intResult.getValue();
        assertEquals(123, intRec.get("selection_int"));
        assertEquals(null, intRec.get("selection_str"));
        assertEquals(null, intRec.get("selection_ref"));
        assertEquals(null, intRec.get("selection_bag"));
        bindAndWrite((GenericRecord) intResult.getValue());

        // ref branch
        ConversionResult refResult = convert("proto-adv-oneof-ref", ADVANCED_PROTO, "AdvancedRecord", b -> {
            Descriptors.FieldDescriptor fd = b.getDescriptorForType().findFieldByName("selection_ref");
            b.setField(fd, nestedMessage(fd.getMessageType(), "sel-ref"));
        });
        GenericRecord refRec = (GenericRecord) refResult.getValue();
        assertEquals("sel-ref", ((GenericRecord) refRec.get("selection_ref")).get("name").toString());
        assertEquals(null, refRec.get("selection_str"));
        assertEquals(null, refRec.get("selection_int"));
        assertEquals(null, refRec.get("selection_bag"));
        bindAndWrite((GenericRecord) refResult.getValue());

        // bag branch (contains repeated refs)
        ConversionResult bagResult = convert("proto-adv-oneof-bag", ADVANCED_PROTO, "AdvancedRecord", b -> {
            Descriptors.FieldDescriptor fd = b.getDescriptorForType().findFieldByName("selection_bag");
            Descriptors.Descriptor bagDesc = fd.getMessageType();
            Descriptors.FieldDescriptor refsFd = bagDesc.findFieldByName("refs");
            DynamicMessage.Builder bagBuilder = DynamicMessage.newBuilder(bagDesc);
            bagBuilder.addRepeatedField(refsFd, nestedMessage(refsFd.getMessageType(), "b1"));
            bagBuilder.addRepeatedField(refsFd, nestedMessage(refsFd.getMessageType(), "b2"));
            b.setField(fd, bagBuilder.build());
        });
        GenericRecord bagRec = (GenericRecord) bagResult.getValue();
        List<?> bagRefs = (List<?>) ((GenericRecord) bagRec.get("selection_bag")).get("refs");
        assertEquals(List.of("b1", "b2"), bagRefs.stream()
            .map(GenericRecord.class::cast)
            .map(r -> r.get("name").toString())
            .collect(Collectors.toList()));
        assertEquals(null, bagRec.get("selection_str"));
        assertEquals(null, bagRec.get("selection_int"));
        assertEquals(null, bagRec.get("selection_ref"));
        bindAndWrite((GenericRecord) bagResult.getValue());

        // int map branch (map<int32,string>)
        ConversionResult intMapResult = convert("proto-adv-oneof-intmap", ADVANCED_PROTO, "AdvancedRecord", b -> {
            Descriptors.FieldDescriptor fd = b.getDescriptorForType().findFieldByName("selection_int_map");
            Descriptors.Descriptor holderDesc = fd.getMessageType();
            Descriptors.FieldDescriptor entriesFd = holderDesc.findFieldByName("entries");
            Descriptors.Descriptor entryDesc = entriesFd.getMessageType();
            DynamicMessage.Builder holder = DynamicMessage.newBuilder(holderDesc);
            holder.addRepeatedField(entriesFd, mapEntry(entryDesc, 10, "x"));
            holder.addRepeatedField(entriesFd, mapEntry(entryDesc, 20, "y"));
            b.setField(fd, holder.build());
        });
        GenericRecord intMapRec = (GenericRecord) intMapResult.getValue();
        Map<Integer, String> intMaps = logicalMapToMap(((GenericRecord) intMapRec.get("selection_int_map")).get("entries"));
        assertEquals(Map.of(10, "x", 20, "y"), intMaps);
        assertEquals(null, intMapRec.get("selection_str"));
        assertEquals(null, intMapRec.get("selection_int"));
        assertEquals(null, intMapRec.get("selection_ref"));
        assertEquals(null, intMapRec.get("selection_bag"));
        bindAndWrite((GenericRecord) intMapResult.getValue());
    }

    @Test
    void convertOptionalCollectionsRecord() throws Exception {
        String topic = "proto-optional-collections";
        ConversionResult result = convert(topic, OPTIONAL_COLLECTION_PROTO, "OptionalCollectionRecord", builder -> {
            Descriptors.FieldDescriptor wrapperFd = builder.getDescriptorForType().findFieldByName("opt_wrapper");
            Descriptors.Descriptor wrapperDesc = wrapperFd.getMessageType();
            Descriptors.Descriptor itemDesc = wrapperDesc.findFieldByName("items").getMessageType();
            DynamicMessage.Builder wrapperBuilder = DynamicMessage.newBuilder(wrapperDesc);
            wrapperBuilder.addRepeatedField(wrapperDesc.findFieldByName("items"), nestedMessage(itemDesc, "i1"));
            wrapperBuilder.addRepeatedField(wrapperDesc.findFieldByName("items"), nestedMessage(itemDesc, "i2"));
            builder.setField(wrapperFd, wrapperBuilder.build());

            // leave opt_int_map unset to validate optional-map -> null

            Descriptors.FieldDescriptor optItemFd = builder.getDescriptorForType().findFieldByName("opt_item");
            builder.setField(optItemFd, nestedMessage(optItemFd.getMessageType(), "single"));
        });

        GenericRecord record = (GenericRecord) result.getValue();
        // opt_wrapper union present
        assertEquals(Schema.Type.UNION, record.getSchema().getField("opt_wrapper").schema().getType());
        List<?> items = (List<?>) ((GenericRecord) record.get("opt_wrapper")).get("items");
        assertEquals(List.of("i1", "i2"), items.stream().map(GenericRecord.class::cast).map(r -> r.get("value").toString()).collect(Collectors.toList()));

        assertEquals(null, record.get("opt_int_map"));

        GenericRecord optItem = (GenericRecord) record.get("opt_item");
        assertEquals("single", optItem.get("value").toString());
    }

    @Test
    void convertOptionalCollectionsRecordWithMap() throws Exception {
        String topic = "proto-optional-collections-map";
        ConversionResult result = convert(topic, OPTIONAL_COLLECTION_PROTO, "OptionalCollectionRecord", builder -> {
            Descriptors.FieldDescriptor optMapFd = builder.getDescriptorForType().findFieldByName("opt_int_map");
            Descriptors.Descriptor holderDesc = optMapFd.getMessageType();
            Descriptors.FieldDescriptor entriesFd = holderDesc.findFieldByName("entries");
            Descriptors.Descriptor entryDesc = entriesFd.getMessageType();
            DynamicMessage.Builder holder = DynamicMessage.newBuilder(holderDesc);
            holder.addRepeatedField(entriesFd, mapEntry(entryDesc, 7, "v7"));
            holder.addRepeatedField(entriesFd, mapEntry(entryDesc, 8, "v8"));
            builder.setField(optMapFd, holder.build());

            Descriptors.FieldDescriptor wrapMapFd = builder.getDescriptorForType().findFieldByName("opt_wrap_map");
            Descriptors.Descriptor wrapMapDesc = wrapMapFd.getMessageType();
            Descriptors.FieldDescriptor wrapEntriesFd = wrapMapDesc.findFieldByName("entries");
            Descriptors.Descriptor wrapEntryDesc = wrapEntriesFd.getMessageType();
            Descriptors.Descriptor wrapValueDesc = wrapEntryDesc.findFieldByName("value").getMessageType();
            Descriptors.Descriptor wrapItemDesc = wrapValueDesc.findFieldByName("items").getMessageType();

            DynamicMessage.Builder wrapHolder = DynamicMessage.newBuilder(wrapMapDesc);
            wrapHolder.addRepeatedField(wrapEntriesFd, mapEntry(
                wrapEntryDesc,
                "wkey1",
                wrapMessage(wrapValueDesc, wrapItemDesc, "wm1-a", List.of("wm1-b"))
            ));
            builder.setField(wrapMapFd, wrapHolder.build());

            // optional timestamp
            Timestamp ts = Timestamp.newBuilder().setSeconds(10L).setNanos(500_000_000).build();
            builder.setField(builder.getDescriptorForType().findFieldByName("opt_ts"), ts);
        });

        GenericRecord record = (GenericRecord) result.getValue();
        GenericRecord optIntMap = (GenericRecord) record.get("opt_int_map");
        Map<Integer, String> map = logicalMapToMap(optIntMap.get("entries"));
        assertEquals(Map.of(7, "v7", 8, "v8"), map);

        Schema.Field optWrapField = record.getSchema().getField("opt_wrap_map");
        assertEquals(Schema.Type.UNION, optWrapField.schema().getType());
        GenericRecord optWrapMap = (GenericRecord) record.get("opt_wrap_map");
        Map<String, GenericRecord> wrapEntries = logicalMapToMap(optWrapMap.get("entries"));
        GenericRecord wrapper = wrapEntries.get("wkey1");
        List<?> wrapItems = (List<?>) wrapper.get("items");
        assertEquals(List.of("wm1-b"), wrapItems.stream()
            .map(GenericRecord.class::cast)
            .map(item -> item.get("value").toString())
            .collect(Collectors.toList()));

        assertEquals(10_500_000L, record.get("opt_ts"));


    }

    @Test
    void convertRecursiveRecord() throws Exception {
        String topic = "proto-recursive";
        ConversionResult result = convert(topic, RECURSIVE_PROTO, "Node", builder -> {
            builder.setField(builder.getDescriptorForType().findFieldByName("id"), "root");
            Descriptors.FieldDescriptor childFd = builder.getDescriptorForType().findFieldByName("child");
            Descriptors.Descriptor childDesc = childFd.getMessageType();
            Descriptors.FieldDescriptor leafFd = childDesc.findFieldByName("leaf");
            Descriptors.Descriptor nodeDesc = leafFd.getMessageType();

            DynamicMessage leaf = DynamicMessage.newBuilder(nodeDesc)
                .setField(nodeDesc.findFieldByName("id"), "leaf")
                .build();
            DynamicMessage child = DynamicMessage.newBuilder(childDesc)
                .setField(leafFd, leaf)
                .build();
            builder.setField(childFd, child);
        });

        GenericRecord record = (GenericRecord) result.getValue();
        assertEquals("root", record.get("id").toString());
        GenericRecord child = (GenericRecord) record.get("child");
        GenericRecord leaf = (GenericRecord) child.get("leaf");
        assertEquals("leaf", leaf.get("id").toString());

        assertThrows(IllegalStateException.class, () -> bindAndWrite(record));
    }

    private ConversionResult convert(String topic, String proto, String messageName, Consumer<DynamicMessage.Builder> messageConfigurer) throws Exception {
        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        CustomProtobufSchema schema = new CustomProtobufSchema(
            messageName,
            -1,
            null,
            null,
            proto,
            List.of(),
            Map.of()
        );
        int schemaId = registryClient.register(topic + "-value", schema);

        ProtoFileElement fileElement = ProtoParser.Companion.parse(ProtoConstants.DEFAULT_LOCATION, proto);
        DynamicSchema dynamicSchema = ProtobufSchemaParser.toDynamicSchema(messageName, fileElement, Collections.emptyMap());
        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor(messageName);

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        messageConfigurer.accept(builder);
        DynamicMessage message = builder.build();

        ByteBuffer payload = buildConfluentPayload(schemaId, message.toByteArray(), 0);
        ProtobufRegistryConverter converter = new ProtobufRegistryConverter(registryClient, "http://mock:8081", false);
        ConversionResult result = converter.convert(topic, payload.asReadOnlyBuffer());

        ConversionResult cached = converter.convert(topic, payload.asReadOnlyBuffer());
        assertSame(result.getSchema(), cached.getSchema());
        return result;
    }

    private void bindAndWrite(GenericRecord record) {
        org.apache.iceberg.Schema iceberg = AvroSchemaUtil.toIceberg(record.getSchema());
        RecordBinder binder = new RecordBinder(iceberg, record.getSchema());
        Record icebergRecord = binder.bind(record);
        assertDoesNotThrow(() -> testSendRecord(iceberg, icebergRecord));
    }

    private static String utf8(Object value) {
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static DynamicMessage nestedMessage(Descriptors.Descriptor descriptor, String value) {
        return DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("note") != null ? descriptor.findFieldByName("note") : descriptor.findFieldByName("value") != null
                ? descriptor.findFieldByName("value") : descriptor.findFieldByName("name"), value)
            .build();
    }

    private static DynamicMessage wrapMessage(Descriptors.Descriptor wrapDesc, Descriptors.Descriptor itemDesc, String itemValue, List<String> itemListValues) {
        DynamicMessage.Builder wrapBuilder = DynamicMessage.newBuilder(wrapDesc);
        Descriptors.FieldDescriptor itemField = wrapDesc.findFieldByName("item");
        if (itemField != null) {
            wrapBuilder.setField(itemField, nestedMessage(itemDesc, itemValue));
        }
        Descriptors.FieldDescriptor itemsFd = wrapDesc.findFieldByName("items");
        if (itemsFd != null) {
            for (String v : itemListValues) {
                wrapBuilder.addRepeatedField(itemsFd, nestedMessage(itemDesc, v));
            }
        }
        return wrapBuilder.build();
    }

    private static DynamicMessage mapEntry(Descriptors.Descriptor descriptor, Object key, Object value) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        builder.setField(descriptor.findFieldByName("key"), key);
        builder.setField(descriptor.findFieldByName("value"), value);
        return builder.build();
    }

    private static <K, V> Map<K, V> logicalMapToMap(Object logicalMap) {
        List<?> entries = (List<?>) logicalMap;
        return entries.stream()
            .map(GenericRecord.class::cast)
            .collect(Collectors.toMap(
                entry -> (K) entry.get("key"),
                entry -> {
                    Object value = entry.get("value");
                    if (value instanceof GenericRecord) {
                        GenericRecord record = (GenericRecord) value;
                        if (record.getSchema().getField("value") != null) {
                            return (V) record.get("value").toString();
                        }
                        if (record.getSchema().getField("name") != null) {
                            return (V) record.get("name").toString();
                        }
                    }
                    return (V) value;
                }
            ));
    }

    private static ByteBuffer buildConfluentPayload(int schemaId, byte[] messageBytes, int... messageIndexes) {
        byte[] indexBytes = encodeMessageIndexes(messageIndexes);
        ByteBuffer buffer = ByteBuffer.allocate(1 + Integer.BYTES + indexBytes.length + messageBytes.length);
        buffer.put((byte) 0);
        buffer.putInt(schemaId);
        buffer.put(indexBytes);
        buffer.put(messageBytes);
        buffer.flip();
        return buffer;
    }

    private static byte[] encodeMessageIndexes(int... indexes) {
        if (indexes == null || indexes.length == 0) {
            return new byte[]{0};
        }
        ByteBuffer buffer = ByteBuffer.allocate(5 * (indexes.length + 1));
        org.apache.kafka.common.utils.ByteUtils.writeVarint(indexes.length, buffer);
        for (int index : indexes) {
            org.apache.kafka.common.utils.ByteUtils.writeVarint(index, buffer);
        }
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private void testSendRecord(org.apache.iceberg.Schema schema, Record record) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
        Table table = catalog.createTable(TableIdentifier.of(Namespace.of("default"), "scenario"), schema);
        TaskWriter<Record> writer = createTableWriter(table);
        try {
            writer.write(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
