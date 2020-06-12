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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;

public class SinkNode<K, V> extends ProcessorNode<K, V> {

    private Serializer<K> keySerializer;
    private Serializer<V> valSerializer;
    private final TopicNameExtractor<K, V> topicExtractor;
    private final StreamPartitioner<? super K, ? super V> partitioner;

    private InternalProcessorContext context;

    SinkNode(final String name,
             final TopicNameExtractor<K, V> topicExtractor,
             final Serializer<K> keySerializer,
             final Serializer<V> valSerializer,
             final StreamPartitioner<? super K, ? super V> partitioner) {
        super(name);

        this.topicExtractor = topicExtractor;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.partitioner = partitioner;
    }

    /**
     * @throws UnsupportedOperationException if this method adds a child to a sink node
     */
    @Override
    public void addChild(final ProcessorNode<?, ?> child) {
        throw new UnsupportedOperationException("sink node does not allow addChild");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final InternalProcessorContext context) {
        super.init(context);
        this.context = context;

        // if serializers are null, get the default ones from the context
        if (keySerializer == null) {
            keySerializer = (Serializer<K>) context.keySerde().serializer();
        }
        if (valSerializer == null) {
            valSerializer = (Serializer<V>) context.valueSerde().serializer();
        }

        // if serializers are internal wrapping serializers that may need to be given the default serializer
        // then pass it the default one from the context
        if (valSerializer instanceof WrappingNullableSerializer) {
            ((WrappingNullableSerializer) valSerializer).setIfUnset(
                context.keySerde().serializer(),
                context.valueSerde().serializer()
            );
        }
    }


    @Override
    public void process(final K key, final V value) {
        final RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();

        final long timestamp = context.timestamp();
        if (timestamp < 0) {
            throw new StreamsException("Invalid (negative) timestamp of " + timestamp + " for output record <" + key + ":" + value + ">.");
        }

        final String topic = topicExtractor.extract(key, value, this.context.recordContext());

        collector.send(topic, key, value, context.headers(), timestamp, keySerializer, valSerializer, partitioner);
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * @return a string representation of this node starting with the given indent, useful for debugging.
     */
    @Override
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder(super.toString(indent));
        sb.append(indent).append("\ttopic:\t\t");
        sb.append(topicExtractor);
        sb.append("\n");
        return sb.toString();
    }

}
