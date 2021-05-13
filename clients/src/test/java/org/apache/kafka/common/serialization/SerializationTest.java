/*
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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SerializationTest {

    final private String topic = "testTopic";
    final private Map<Class<?>, List<Object>> testData = new HashMap<Class<?>, List<Object>>() {
        {
            put(String.class, Arrays.asList("my string"));
            put(Short.class, Arrays.asList((short) 32767, (short) -32768));
            put(Integer.class, Arrays.asList(423412424, -41243432));
            put(Long.class, Arrays.asList(922337203685477580L, -922337203685477581L));
            put(Float.class, Arrays.asList(5678567.12312f, -5678567.12341f));
            put(Double.class, Arrays.asList(5678567.12312d, -5678567.12341d));
            put(byte[].class, Arrays.asList("my string".getBytes()));
            put(ByteBuffer.class, Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes())));
            put(Bytes.class, Arrays.asList(new Bytes("my string".getBytes())));
            put(UUID.class, Arrays.asList(UUID.randomUUID()));
        }
    };

    private class DummyClass {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void allSerdesShouldRoundtripInput() {
        for (Map.Entry<Class<?>, List<Object>> test : testData.entrySet()) {
            try (Serde<Object> serde = Serdes.serdeFrom((Class<Object>) test.getKey())) {
                for (Object value : test.getValue()) {
                    assertEquals(value, serde.deserializer().deserialize(topic, serde.serializer().serialize(topic, value)),
                        "Should get the original " + test.getKey().getSimpleName() + " after serialization and deserialization");
                }
            }
        }
    }

    @Test
    public void allSerdesShouldSupportNull() {
        for (Class<?> cls : testData.keySet()) {
            try (Serde<?> serde = Serdes.serdeFrom(cls)) {
                assertNull(serde.serializer().serialize(topic, null),
                    "Should support null in " + cls.getSimpleName() + " serialization");
                assertNull(serde.deserializer().deserialize(topic, null),
                    "Should support null in " + cls.getSimpleName() + " deserialization");
            }
        }
    }

    @Test
    public void testSerdeFromUnknown() {
        assertThrows(IllegalArgumentException.class, () -> Serdes.serdeFrom(DummyClass.class));
    }

    @Test
    public void testSerdeFromNotNull() {
        try (Serde<Long> serde = Serdes.Long()) {
            assertThrows(IllegalArgumentException.class, () -> Serdes.serdeFrom(null, serde.deserializer()));
        }
    }

    @Test
    public void stringSerdeShouldSupportDifferentEncodings() {
        String str = "my string";
        List<String> encodings = Arrays.asList(StandardCharsets.UTF_8.name(), StandardCharsets.UTF_16.name());

        for (String encoding : encodings) {
            try (Serde<String> serDeser = getStringSerde(encoding)) {

                Serializer<String> serializer = serDeser.serializer();
                Deserializer<String> deserializer = serDeser.deserializer();
                assertEquals(str, deserializer.deserialize(topic, serializer.serialize(topic, str)),
                    "Should get the original string after serialization and deserialization with encoding " + encoding);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldReturnEmptyCollection() {
        List<Integer> testData = Arrays.asList();
        Serde<List<Integer>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Integer());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get empty collection after serialization and deserialization on an empty list");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldReturnNull() {
        List<Integer> testData = null;
        Serde<List<Integer>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Integer());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get null after serialization and deserialization on an empty list");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripIntPrimitiveInput() {
        List<Integer> testData = Arrays.asList(1, 2, 3);
        Serde<List<Integer>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Integer());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of integer primitives after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeSerializerShouldReturnByteArrayOfFixedSizeForIntPrimitiveInput() {
        List<Integer> testData = Arrays.asList(1, 2, 3);
        Serde<List<Integer>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Integer());
        assertEquals(21, listSerde.serializer().serialize(topic, testData).length,
            "Should get length of 21 bytes after serialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripShortPrimitiveInput() {
        List<Short> testData = Arrays.asList((short) 1, (short) 2, (short) 3);
        Serde<List<Short>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Short());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of short primitives after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeSerializerShouldReturnByteArrayOfFixedSizeForShortPrimitiveInput() {
        List<Short> testData = Arrays.asList((short) 1, (short) 2, (short) 3);
        Serde<List<Short>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Short());
        assertEquals(15, listSerde.serializer().serialize(topic, testData).length,
            "Should get length of 15 bytes after serialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripFloatPrimitiveInput() {
        List<Float> testData = Arrays.asList((float) 1, (float) 2, (float) 3);
        Serde<List<Float>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Float());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of float primitives after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeSerializerShouldReturnByteArrayOfFixedSizeForFloatPrimitiveInput() {
        List<Float> testData = Arrays.asList((float) 1, (float) 2, (float) 3);
        Serde<List<Float>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Float());
        assertEquals(21, listSerde.serializer().serialize(topic, testData).length,
            "Should get length of 21 bytes after serialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripLongPrimitiveInput() {
        List<Long> testData = Arrays.asList((long) 1, (long) 2, (long) 3);
        Serde<List<Long>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Long());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of long primitives after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeSerializerShouldReturnByteArrayOfFixedSizeForLongPrimitiveInput() {
        List<Long> testData = Arrays.asList((long) 1, (long) 2, (long) 3);
        Serde<List<Long>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Long());
        assertEquals(33, listSerde.serializer().serialize(topic, testData).length,
            "Should get length of 33 bytes after serialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripDoublePrimitiveInput() {
        List<Double> testData = Arrays.asList((double) 1, (double) 2, (double) 3);
        Serde<List<Double>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Double());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of double primitives after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeSerializerShouldReturnByteArrayOfFixedSizeForDoublePrimitiveInput() {
        List<Double> testData = Arrays.asList((double) 1, (double) 2, (double) 3);
        Serde<List<Double>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Double());
        assertEquals(33, listSerde.serializer().serialize(topic, testData).length,
            "Should get length of 33 bytes after serialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripUUIDInput() {
        List<UUID> testData = Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        Serde<List<UUID>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.UUID());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of UUID after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeSerializerShouldReturnByteArrayOfFixedSizeForUUIDInput() {
        List<UUID> testData = Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        Serde<List<UUID>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.UUID());
        assertEquals(117, listSerde.serializer().serialize(topic, testData).length,
            "Should get length of 117 bytes after serialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripNonPrimitiveInput() {
        List<String> testData = Arrays.asList("A", "B", "C");
        Serde<List<String>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.String());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of strings list after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripPrimitiveInputWithNullEntries() {
        List<Integer> testData = Arrays.asList(1, null, 3);
        Serde<List<Integer>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.Integer());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of integer primitives with null entries "
                + "after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldRoundtripNonPrimitiveInputWithNullEntries() {
        List<String> testData = Arrays.asList("A", null, "C");
        Serde<List<String>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.String());
        assertEquals(testData,
            listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData)),
            "Should get the original collection of strings list with null entries "
                + "after serialization and deserialization");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldReturnLinkedList() {
        List<Integer> testData = new LinkedList<>();
        Serde<List<Integer>> listSerde = Serdes.ListSerde(LinkedList.class, Serdes.Integer());
        assertTrue(listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData))
            instanceof LinkedList, "Should return List instance of type LinkedList");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void listSerdeShouldReturnStack() {
        List<Integer> testData = new Stack<>();
        Serde<List<Integer>> listSerde = Serdes.ListSerde(Stack.class, Serdes.Integer());
        assertTrue(listSerde.deserializer().deserialize(topic, listSerde.serializer().serialize(topic, testData))
            instanceof Stack, "Should return List instance of type Stack");
    }

    @Test
    public void floatDeserializerShouldThrowSerializationExceptionOnZeroBytes() {
        try (Serde<Float> serde = Serdes.Float()) {
            assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, new byte[0]));
        }
    }

    @Test
    public void floatDeserializerShouldThrowSerializationExceptionOnTooFewBytes() {
        try (Serde<Float> serde = Serdes.Float()) {
            assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, new byte[3]));
        }
    }


    @Test
    public void floatDeserializerShouldThrowSerializationExceptionOnTooManyBytes() {
        try (Serde<Float> serde = Serdes.Float()) {
            assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, new byte[5]));
        }
    }

    @Test
    public void floatSerdeShouldPreserveNaNValues() {
        int someNaNAsIntBits = 0x7f800001;
        float someNaN = Float.intBitsToFloat(someNaNAsIntBits);
        int anotherNaNAsIntBits = 0x7f800002;
        float anotherNaN = Float.intBitsToFloat(anotherNaNAsIntBits);

        try (Serde<Float> serde = Serdes.Float()) {
            // Because of NaN semantics we must assert based on the raw int bits.
            Float roundtrip = serde.deserializer().deserialize(topic,
                    serde.serializer().serialize(topic, someNaN));
            assertEquals(someNaNAsIntBits, Float.floatToRawIntBits(roundtrip));
            Float otherRoundtrip = serde.deserializer().deserialize(topic,
                    serde.serializer().serialize(topic, anotherNaN));
            assertEquals(anotherNaNAsIntBits, Float.floatToRawIntBits(otherRoundtrip));
        }
    }

    @Test
    public void testSerializeVoid() {
        try (Serde<Void> serde = Serdes.Void()) {
            serde.serializer().serialize(topic, null);
        }
    }

    @Test
    public void testDeserializeVoid() {
        try (Serde<Void> serde = Serdes.Void()) {
            serde.deserializer().deserialize(topic, null);
        }
    }

    @Test
    public void voidDeserializerShouldThrowOnNotNullValues() {
        try (Serde<Void> serde = Serdes.Void()) {
            assertThrows(IllegalArgumentException.class, () -> serde.deserializer().deserialize(topic, new byte[5]));
        }
    }

    private Serde<String> getStringSerde(String encoder) {
        Map<String, Object> serializerConfigs = new HashMap<String, Object>();
        serializerConfigs.put("key.serializer.encoding", encoder);
        Serializer<String> serializer = Serdes.String().serializer();
        serializer.configure(serializerConfigs, true);

        Map<String, Object> deserializerConfigs = new HashMap<String, Object>();
        deserializerConfigs.put("key.deserializer.encoding", encoder);
        Deserializer<String> deserializer = Serdes.String().deserializer();
        deserializer.configure(deserializerConfigs, true);

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
