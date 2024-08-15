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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

public class SinkNodeTest {
    private final StateSerdes<Bytes, Bytes> anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes.class, Bytes.class);
    private final Serializer<byte[]> anySerializer = Serdes.ByteArray().serializer();
    private final RecordCollector recordCollector = new MockRecordCollector();
    private final InternalMockProcessorContext<Void, Void> context = new InternalMockProcessorContext<>(anyStateSerde, recordCollector);
    private final SinkNode<byte[], byte[]> sink = new SinkNode<>("anyNodeName",
            new StaticTopicNameExtractor<>("any-output-topic"), anySerializer, anySerializer, null);

    // Used to verify that the correct exceptions are thrown if the compiler checks are bypassed
    @SuppressWarnings({"unchecked", "rawtypes"})
    private final SinkNode<Object, Object> illTypedSink = (SinkNode) sink;
    private MockedStatic<WrappingNullableUtils> utilsMock;

    @BeforeEach
    public void setup() {
        utilsMock = Mockito.mockStatic(WrappingNullableUtils.class);
    }

    @AfterEach
    public void cleanup() {
        utilsMock.close();
    }

    @Test
    public void shouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp() {
        sink.init(context);
        // When/Then
        context.setTime(-1); // ensures a negative timestamp is set for the record we send next
        try {
            illTypedSink.process(new Record<>("any key".getBytes(), "any value".getBytes(), -1));
            fail("Should have thrown StreamsException");
        } catch (final StreamsException ignored) {
            // expected
        }
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedKeySerde() {
        utilsMock.when(() -> WrappingNullableUtils.prepareKeySerializer(any(), any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class, () -> sink.init(context));

        assertThat(
            exception.getMessage(),
            equalTo("Failed to initialize key serdes for sink node anyNodeName")
        );
        assertThat(
            exception.getCause().getMessage(),
            equalTo("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedValueSerde() {
        utilsMock.when(() -> WrappingNullableUtils.prepareValueSerializer(any(), any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class, () -> sink.init(context));

        assertThat(
            exception.getMessage(),
            equalTo("Failed to initialize value serdes for sink node anyNodeName")
        );
        assertThat(
            exception.getCause().getMessage(),
            equalTo("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionWithExplicitErrorMessage() {
        utilsMock.when(() -> WrappingNullableUtils.prepareKeySerializer(any(), any(), any())).thenThrow(new StreamsException(""));

        final Throwable exception = assertThrows(StreamsException.class, () -> sink.init(context));

        assertThat(exception.getMessage(), equalTo("Failed to initialize key serdes for sink node anyNodeName"));
    }
}
