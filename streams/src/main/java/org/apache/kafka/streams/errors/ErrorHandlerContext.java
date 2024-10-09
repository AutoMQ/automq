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
package org.apache.kafka.streams.errors;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;

/**
 * This interface allows user code to inspect the context of a record that has failed processing.
 */
public interface ErrorHandlerContext {
    /**
     * Return the topic name of the current input record; could be {@code null} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated topic.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid topic name, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the topic name
     */
    String topic();

    /**
     * Return the partition ID of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated partition ID.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid partition ID, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the partition ID
     */
    int partition();

    /**
     * Return the offset of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated offset.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid offset, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the offset
     */
    long offset();

    /**
     * Return the headers of the current source record; could be an empty header if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record might not have any associated headers.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide valid headers, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the headers
     */
    Headers headers();

    /**
     * Return the non-deserialized byte[] of the input message key if the context has been triggered by a message.
     *
     * <p> If this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, it will return {@code null}.
     *
     * <p> If this method is invoked in a sub-topology due to a repartition, the returned key would be one sent
     * to the repartition topic.
     *
     * @return the raw byte of the key of the source message
     */
    byte[] sourceRawKey();

    /**
     * Return the non-deserialized byte[] of the input message value if the context has been triggered by a message.
     *
     * <p> If this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, it will return {@code null}.
     *
     * <p> If this method is invoked in a sub-topology due to a repartition, the returned value would be one sent
     * to the repartition topic.
     *
     * @return the raw byte of the value of the source message
     */
    byte[] sourceRawValue();

    /**
     * Return the current processor node ID.
     *
     * @return the processor node ID
     */
    String processorNodeId();

    /**
     * Return the task ID.
     *
     * @return the task ID
     */
    TaskId taskId();
}
