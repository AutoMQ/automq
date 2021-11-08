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
package org.apache.kafka.streams.kstream.internals;

import static org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.optimizableRepartitionNodeBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

class CogroupedStreamAggregateBuilder<K, VOut> {
    private final InternalStreamsBuilder builder;
    private final Map<KGroupedStreamImpl<K, ?>, GraphNode> parentNodes = new LinkedHashMap<>();

    CogroupedStreamAggregateBuilder(final InternalStreamsBuilder builder) {
        this.builder = builder;
    }
    <KR> KTable<KR, VOut> build(final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns,
                                final Initializer<VOut> initializer,
                                final NamedInternal named,
                                final StoreBuilder<?> storeBuilder,
                                final Serde<KR> keySerde,
                                final Serde<VOut> valueSerde,
                                final String queryableName) {
        processRepartitions(groupPatterns, storeBuilder);
        final Collection<GraphNode> processors = new ArrayList<>();
        final Collection<KStreamAggProcessorSupplier> parentProcessors = new ArrayList<>();
        boolean stateCreated = false;
        int counter = 0;
        for (final Entry<KGroupedStreamImpl<K, ?>, Aggregator<? super K, Object, VOut>> kGroupedStream : groupPatterns.entrySet()) {
            final KStreamAggProcessorSupplier<K, ?, K, ?> parentProcessor =
                new KStreamAggregate<>(storeBuilder.name(), initializer, kGroupedStream.getValue());
            parentProcessors.add(parentProcessor);
            final StatefulProcessorNode<K, ?> statefulProcessorNode = getStatefulProcessorNode(
                named.suffixWithOrElseGet(
                    "-cogroup-agg-" + counter++,
                    builder,
                    CogroupedKStreamImpl.AGGREGATE_NAME),
                stateCreated,
                storeBuilder,
                parentProcessor);
            stateCreated = true;
            processors.add(statefulProcessorNode);
            builder.addGraphNode(parentNodes.get(kGroupedStream.getKey()), statefulProcessorNode);
        }
        return createTable(processors, parentProcessors, named, keySerde, valueSerde, queryableName, storeBuilder.name());
    }

    @SuppressWarnings("unchecked")
    <KR, W extends Window> KTable<KR, VOut> build(final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns,
                                                  final Initializer<VOut> initializer,
                                                  final NamedInternal named,
                                                  final StoreBuilder<?> storeBuilder,
                                                  final Serde<KR> keySerde,
                                                  final Serde<VOut> valueSerde,
                                                  final String queryableName,
                                                  final Windows<W> windows) {
        processRepartitions(groupPatterns, storeBuilder);

        final Collection<GraphNode> processors = new ArrayList<>();
        final Collection<KStreamAggProcessorSupplier> parentProcessors = new ArrayList<>();
        boolean stateCreated = false;
        int counter = 0;
        for (final Entry<KGroupedStreamImpl<K, ?>, Aggregator<? super K, Object, VOut>> kGroupedStream : groupPatterns.entrySet()) {
            final KStreamAggProcessorSupplier<K, ?, K, ?>  parentProcessor =
                (KStreamAggProcessorSupplier<K, ?, K, ?>) new KStreamWindowAggregate<K, K, VOut, W>(
                    windows,
                    storeBuilder.name(),
                    initializer,
                    kGroupedStream.getValue());
            parentProcessors.add(parentProcessor);
            final StatefulProcessorNode<K, ?> statefulProcessorNode = getStatefulProcessorNode(
                named.suffixWithOrElseGet(
                    "-cogroup-agg-" + counter++,
                    builder,
                    CogroupedKStreamImpl.AGGREGATE_NAME),
                stateCreated,
                storeBuilder,
                parentProcessor);
            stateCreated = true;
            processors.add(statefulProcessorNode);
            builder.addGraphNode(parentNodes.get(kGroupedStream.getKey()), statefulProcessorNode);
        }
        return createTable(processors, parentProcessors, named, keySerde, valueSerde, queryableName, storeBuilder.name());
    }

    @SuppressWarnings("unchecked")
    <KR> KTable<KR, VOut> build(final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns,
                                final Initializer<VOut> initializer,
                                final NamedInternal named,
                                final StoreBuilder<?> storeBuilder,
                                final Serde<KR> keySerde,
                                final Serde<VOut> valueSerde,
                                final String queryableName,
                                final SessionWindows sessionWindows,
                                final Merger<? super K, VOut> sessionMerger) {
        processRepartitions(groupPatterns, storeBuilder);
        final Collection<GraphNode> processors = new ArrayList<>();
        final Collection<KStreamAggProcessorSupplier> parentProcessors = new ArrayList<>();
        boolean stateCreated = false;
        int counter = 0;
        for (final Entry<KGroupedStreamImpl<K, ?>, Aggregator<? super K, Object, VOut>> kGroupedStream : groupPatterns.entrySet()) {
            final KStreamAggProcessorSupplier<K, ?, K, ?> parentProcessor =
                (KStreamAggProcessorSupplier<K, ?, K, ?>) new KStreamSessionWindowAggregate<K, K, VOut>(
                    sessionWindows,
                    storeBuilder.name(),
                    initializer,
                    kGroupedStream.getValue(),
                    sessionMerger);
            parentProcessors.add(parentProcessor);
            final StatefulProcessorNode<K, ?> statefulProcessorNode = getStatefulProcessorNode(
                named.suffixWithOrElseGet(
                    "-cogroup-agg-" + counter++,
                    builder,
                    CogroupedKStreamImpl.AGGREGATE_NAME),
                stateCreated,
                storeBuilder,
                parentProcessor);
            stateCreated = true;
            processors.add(statefulProcessorNode);
            builder.addGraphNode(parentNodes.get(kGroupedStream.getKey()), statefulProcessorNode);
        }
        return createTable(processors, parentProcessors, named, keySerde, valueSerde, queryableName, storeBuilder.name());
    }

    @SuppressWarnings("unchecked")
    <KR> KTable<KR, VOut> build(final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns,
                                final Initializer<VOut> initializer,
                                final NamedInternal named,
                                final StoreBuilder<?> storeBuilder,
                                final Serde<KR> keySerde,
                                final Serde<VOut> valueSerde,
                                final String queryableName,
                                final SlidingWindows slidingWindows) {
        processRepartitions(groupPatterns, storeBuilder);
        final Collection<KStreamAggProcessorSupplier> parentProcessors = new ArrayList<>();
        final Collection<GraphNode> processors = new ArrayList<>();
        boolean stateCreated = false;
        int counter = 0;
        for (final Entry<KGroupedStreamImpl<K, ?>, Aggregator<? super K, Object, VOut>> kGroupedStream : groupPatterns.entrySet()) {
            final KStreamAggProcessorSupplier<K, ?, K, ?> parentProcessor =
                (KStreamAggProcessorSupplier<K, ?, K, ?>) new KStreamSlidingWindowAggregate<K, K, VOut>(
                    slidingWindows,
                    storeBuilder.name(),
                    initializer,
                    kGroupedStream.getValue());
            parentProcessors.add(parentProcessor);
            final StatefulProcessorNode<K, ?> statefulProcessorNode = getStatefulProcessorNode(
                named.suffixWithOrElseGet(
                    "-cogroup-agg-" + counter++,
                    builder,
                    CogroupedKStreamImpl.AGGREGATE_NAME),
                stateCreated,
                storeBuilder,
                parentProcessor);
            stateCreated = true;
            processors.add(statefulProcessorNode);
            builder.addGraphNode(parentNodes.get(kGroupedStream.getKey()), statefulProcessorNode);
        }
        return createTable(processors, parentProcessors, named, keySerde, valueSerde, queryableName, storeBuilder.name());
    }

    private void processRepartitions(final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, VOut>> groupPatterns,
                                     final StoreBuilder<?> storeBuilder) {
        for (final KGroupedStreamImpl<K, ?> repartitionReqs : groupPatterns.keySet()) {

            if (repartitionReqs.repartitionRequired) {

                final OptimizableRepartitionNodeBuilder<K, ?> repartitionNodeBuilder = optimizableRepartitionNodeBuilder();

                final String repartitionNamePrefix = repartitionReqs.userProvidedRepartitionTopicName != null ?
                    repartitionReqs.userProvidedRepartitionTopicName : storeBuilder.name();

                createRepartitionSource(repartitionNamePrefix, repartitionNodeBuilder, repartitionReqs.keySerde, repartitionReqs.valueSerde);

                if (!parentNodes.containsKey(repartitionReqs)) {
                    final GraphNode repartitionNode = repartitionNodeBuilder.build();
                    builder.addGraphNode(repartitionReqs.graphNode, repartitionNode);
                    parentNodes.put(repartitionReqs, repartitionNode);
                }
            } else {
                parentNodes.put(repartitionReqs, repartitionReqs.graphNode);
            }
        }

        final Collection<? extends AbstractStream<K, ?>> groupedStreams = new ArrayList<>(parentNodes.keySet());
        final AbstractStream<K, ?> kGrouped = groupedStreams.iterator().next();
        groupedStreams.remove(kGrouped);
        kGrouped.ensureCopartitionWith(groupedStreams);

    }

    @SuppressWarnings("unchecked")
    <KR, VIn> KTable<KR, VOut> createTable(final Collection<GraphNode> processors,
                                           final Collection<KStreamAggProcessorSupplier> parentProcessors,
                                           final NamedInternal named,
                                           final Serde<KR> keySerde,
                                           final Serde<VOut> valueSerde,
                                           final String queryableName,
                                           final String storeName) {

        final String mergeProcessorName = named.suffixWithOrElseGet(
            "-cogroup-merge",
            builder,
            CogroupedKStreamImpl.MERGE_NAME);
        final KTableProcessorSupplier<K, VOut, K, VOut> passThrough = new KTablePassThrough<>(parentProcessors, storeName);
        final ProcessorParameters<K, VOut, ?, ?> processorParameters = new ProcessorParameters(passThrough, mergeProcessorName);
        final ProcessorGraphNode<K, VOut> mergeNode =
            new ProcessorGraphNode<>(mergeProcessorName, processorParameters);

        builder.addGraphNode(processors, mergeNode);

        return new KTableImpl<KR, VIn, VOut>(
            mergeProcessorName,
            keySerde,
            valueSerde,
            Collections.singleton(mergeNode.nodeName()),
            queryableName,
            passThrough,
            mergeNode,
            builder);
    }

    private StatefulProcessorNode<K, ?> getStatefulProcessorNode(final String processorName,
                                                                 final boolean stateCreated,
                                                                 final StoreBuilder<?> storeBuilder,
                                                                 final ProcessorSupplier<K, ?, K, ?> kStreamAggregate) {
        final StatefulProcessorNode<K, ?> statefulProcessorNode;
        if (!stateCreated) {
            statefulProcessorNode =
                new StatefulProcessorNode<>(
                    processorName,
                    new ProcessorParameters<>(kStreamAggregate, processorName),
                    storeBuilder
                );
        } else {
            statefulProcessorNode =
                new StatefulProcessorNode<>(
                    processorName,
                    new ProcessorParameters<>(kStreamAggregate, processorName),
                    new String[]{storeBuilder.name()}
                );
        }

        return statefulProcessorNode;
    }

    @SuppressWarnings("unchecked")
    private <VIn> void createRepartitionSource(final String repartitionTopicNamePrefix,
                                               final OptimizableRepartitionNodeBuilder<K, ?> optimizableRepartitionNodeBuilder,
                                               final Serde<K> keySerde,
                                               final Serde<?> valueSerde) {

        KStreamImpl.createRepartitionedSource(builder,
            keySerde,
            (Serde<VIn>) valueSerde,
            repartitionTopicNamePrefix,
            null,
            (OptimizableRepartitionNodeBuilder<K, VIn>) optimizableRepartitionNodeBuilder);

    }
}
