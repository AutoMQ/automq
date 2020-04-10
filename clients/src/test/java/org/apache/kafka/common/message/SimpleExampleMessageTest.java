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
package org.apache.kafka.common.message;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SimpleExampleMessageTest {

    @Test
    public void shouldStoreField() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});

        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf);

        assertEquals(uuid, out.processId());
        assertEquals(buf, out.zeroCopyByteBuffer());

        out.setNullableZeroCopyByteBuffer(null);
        assertNull(out.nullableZeroCopyByteBuffer());
        out.setNullableZeroCopyByteBuffer(buf);
        assertEquals(buf, out.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldThrowIfCannotWriteNonIgnorableField() {
        // processId is not supported in v0 and is not marked as ignorable

        final SimpleExampleMessageData out = new SimpleExampleMessageData().setProcessId(UUID.randomUUID());
        assertThrows(UnsupportedVersionException.class, () ->
                out.write(new ByteBufferAccessor(ByteBuffer.allocate(64)), new ObjectSerializationCache(), (short) 0));
        assertThrows(UnsupportedVersionException.class, () -> out.toStruct((short) 0));
    }

    @Test
    public void shouldDefaultField() {
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000000"), out.processId());
        assertEquals(ByteUtils.EMPTY_BUF, out.zeroCopyByteBuffer());
        assertEquals(ByteUtils.EMPTY_BUF, out.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughStruct() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf);

        final Struct struct = out.toStruct((short) 1);
        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.fromStruct(struct, (short) 1);

        buf.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf, in.zeroCopyByteBuffer());
        assertEquals(ByteUtils.EMPTY_BUF, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughStructWithNullable() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf1 = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final ByteBuffer buf2 = ByteBuffer.wrap(new byte[] {4, 5, 6});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf1);
        out.setNullableZeroCopyByteBuffer(buf2);

        final Struct struct = out.toStruct((short) 1);
        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.fromStruct(struct, (short) 1);

        buf1.rewind();
        buf2.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf1, in.zeroCopyByteBuffer());
        assertEquals(buf2, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughBuffer() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        final ByteBuffer buffer = ByteBuffer.allocate(out.size(cache, (short) 1));
        out.write(new ByteBufferAccessor(buffer), cache, (short) 1);
        buffer.rewind();

        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        buf.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf, in.zeroCopyByteBuffer());
        assertEquals(ByteUtils.EMPTY_BUF, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughBufferWithNullable() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf1 = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final ByteBuffer buf2 = ByteBuffer.wrap(new byte[] {4, 5, 6});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf1);
        out.setNullableZeroCopyByteBuffer(buf2);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        final ByteBuffer buffer = ByteBuffer.allocate(out.size(cache, (short) 1));
        out.write(new ByteBufferAccessor(buffer), cache, (short) 1);
        buffer.rewind();

        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        buf1.rewind();
        buf2.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf1, in.zeroCopyByteBuffer());
        assertEquals(buf2, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldImplementEqualsAndHashCode() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final SimpleExampleMessageData a = new SimpleExampleMessageData();
        a.setProcessId(uuid);
        a.setZeroCopyByteBuffer(buf);

        final SimpleExampleMessageData b = new SimpleExampleMessageData();
        b.setProcessId(uuid);
        b.setZeroCopyByteBuffer(buf);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        // just tagging this on here
        assertEquals(a.toString(), b.toString());

        a.setNullableZeroCopyByteBuffer(buf);
        b.setNullableZeroCopyByteBuffer(buf);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());

        a.setNullableZeroCopyByteBuffer(null);
        b.setNullableZeroCopyByteBuffer(null);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());
    }

    @Test
    public void testMyTaggedIntArray() {
        // Verify that the tagged int array reads as empty when not set.
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertEquals(Collections.emptyList(), message.myTaggedIntArray()));

        // Verify that we can set a tagged array of ints.
        testRoundTrip(new SimpleExampleMessageData().
                setMyTaggedIntArray(Arrays.asList(1, 2, 3)),
            message -> assertEquals(Arrays.asList(1, 2, 3), message.myTaggedIntArray()));
    }

    @Test
    public void testMyNullableString() {
        // Verify that the tagged field reads as null when not set.
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertTrue(message.myNullableString() == null));

        // Verify that we can set and retrieve a string for the tagged field.
        testRoundTrip(new SimpleExampleMessageData().setMyNullableString("foobar"),
            message -> assertEquals("foobar", message.myNullableString()));
    }

    @Test
    public void testMyInt16() {
        // Verify that the tagged field reads as 123 when not set.
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertEquals((short) 123, message.myInt16()));

        testRoundTrip(new SimpleExampleMessageData().setMyInt16((short) 456),
            message -> assertEquals((short) 456, message.myInt16()));
    }

    @Test
    public void testMyString() {
        // Verify that the tagged field reads as empty when not set.
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertEquals("", message.myString()));

        testRoundTrip(new SimpleExampleMessageData().setMyString("abc"),
            message -> assertEquals("abc", message.myString()));
    }

    @Test
    public void testMyBytes() {
        // Verify that the tagged field reads as empty when not set.
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertArrayEquals(new byte[0], message.myBytes()));

        testRoundTrip(new SimpleExampleMessageData().
                setMyBytes(new byte[] {0x43, 0x66}),
            message -> assertArrayEquals(new byte[] {0x43, 0x66},
                message.myBytes()));

        testRoundTrip(new SimpleExampleMessageData().setMyBytes(null),
            message -> assertTrue(message.myBytes() == null));
    }

    @Test
    public void testTaggedUuid() {
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertEquals(
                UUID.fromString("212d5494-4a8b-4fdf-94b3-88b470beb367"),
                message.taggedUuid()));

        testRoundTrip(new SimpleExampleMessageData().
                setTaggedUuid(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
            message -> assertEquals(
                UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"),
                message.taggedUuid()));
    }

    @Test
    public void testTaggedLong() {
        testRoundTrip(new SimpleExampleMessageData(),
            message -> assertEquals(0xcafcacafcacafcaL,
                message.taggedLong()));

        testRoundTrip(new SimpleExampleMessageData().
                setMyString("blah").
                setMyTaggedIntArray(Arrays.asList(4)).
                setTaggedLong(0x123443211234432L),
            message -> assertEquals(0x123443211234432L,
                message.taggedLong()));
    }

    @Test
    public void testMyStruct() {
        // Verify that we can set and retrieve a nullable struct object.
        SimpleExampleMessageData.MyStruct myStruct =
            new SimpleExampleMessageData.MyStruct().setStructId(10).setArrayInStruct(
                Collections.singletonList(new SimpleExampleMessageData.StructArray().setArrayFieldId(20))
            );
        testRoundTrip(new SimpleExampleMessageData().setMyStruct(myStruct),
            message -> assertEquals(myStruct, message.myStruct()), (short) 2);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testMyStructUnsupportedVersion() {
        SimpleExampleMessageData.MyStruct myStruct =
                new SimpleExampleMessageData.MyStruct().setStructId(10);
        // Check serialization throws exception for unsupported version
        testRoundTrip(new SimpleExampleMessageData().setMyStruct(myStruct),
            __ -> { }, (short) 1);
    }

    /**
     * Check following cases:
     * 1. Tagged struct can be serialized/deserialized for version it is supported
     * 2. Tagged struct doesn't matter for versions it is not declared.
     */
    @Test
    public void testMyTaggedStruct() {
        // Verify that we can set and retrieve a nullable struct object.
        SimpleExampleMessageData.MyTaggedStruct myStruct =
            new SimpleExampleMessageData.MyTaggedStruct().setStructId("abc");
        testRoundTrip(new SimpleExampleMessageData().setMyTaggedStruct(myStruct),
            message -> assertEquals(myStruct, message.myTaggedStruct()), (short) 2);

        // Not setting field works for both version 1 and version 2 protocol
        testRoundTrip(new SimpleExampleMessageData().setMyString("abc"),
            message -> assertEquals("abc", message.myString()), (short) 1);
        testRoundTrip(new SimpleExampleMessageData().setMyString("abc"),
            message -> assertEquals("abc", message.myString()), (short) 2);
    }

    private void testRoundTrip(SimpleExampleMessageData message,
                               Consumer<SimpleExampleMessageData> validator) {
        testRoundTrip(message, validator, (short) 1);
    }

    private void testRoundTrip(SimpleExampleMessageData message,
                               Consumer<SimpleExampleMessageData> validator,
                               short version) {
        validator.accept(message);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        message.write(new ByteBufferAccessor(buf), cache, version);
        buf.flip();
        assertEquals(size, buf.remaining());

        SimpleExampleMessageData message2 = new SimpleExampleMessageData();
        message2.read(new ByteBufferAccessor(buf.duplicate()), version);
        validator.accept(message2);
        assertEquals(message, message2);
        assertEquals(message.hashCode(), message2.hashCode());
    }
}
