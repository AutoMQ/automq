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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;

import java.util.Map;

public final class StoreQueryUtils {

    // make this class uninstantiable
    private StoreQueryUtils() {
    }

    @SuppressWarnings("unchecked")
    public static <R> QueryResult<R> handleBasicQueries(
        final Query<R> query,
        final PositionBound positionBound,
        final boolean collectExecutionInfo,
        final StateStore store,
        final Position position,
        final int partition
    ) {

        final QueryResult<R> result;
        final long start = collectExecutionInfo ? System.nanoTime() : -1L;
        if (query instanceof PingQuery) {
            if (!isPermitted(position, positionBound, partition)) {
                result = QueryResult.notUpToBound(position, positionBound, partition);
            } else {
                result = (QueryResult<R>) QueryResult.forResult(true);
            }
        } else {
            result = QueryResult.forUnknownQueryType(query, store);
        }
        if (collectExecutionInfo) {
            result.addExecutionInfo(
                "Handled in " + store.getClass() + " in " + (System.nanoTime() - start) + "ns"
            );
        }
        result.setPosition(position);
        return result;
    }

    public static void updatePosition(
        final Position position,
        final StateStoreContext stateStoreContext) {

        if (stateStoreContext != null && stateStoreContext.recordMetadata().isPresent()) {
            final RecordMetadata meta = stateStoreContext.recordMetadata().get();
            position.withComponent(meta.topic(), meta.partition(), meta.offset());
        }
    }

    public static boolean isPermitted(
        final Position position,
        final PositionBound positionBound,
        final int partition
    ) {
        final Position bound = positionBound.position();
        for (final String topic : bound.getTopics()) {
            final Map<Integer, Long> partitionBounds = bound.getPartitionPositions(topic);
            final Map<Integer, Long> seenPartitionPositions = position.getPartitionPositions(topic);
            if (!partitionBounds.containsKey(partition)) {
                // this topic isn't bounded for our partition, so just skip over it.
            } else {
                if (!seenPartitionPositions.containsKey(partition)) {
                    // we haven't seen a partition that we have a bound for
                    return false;
                } else if (seenPartitionPositions.get(partition) < partitionBounds.get(partition)) {
                    // our current position is behind the bound
                    return false;
                }
            }
        }
        return true;
    }
}
