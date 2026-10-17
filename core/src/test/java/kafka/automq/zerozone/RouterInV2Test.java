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

package kafka.automq.zerozone;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Verifies the ordering and progress guarantees of RouterIn V2 read completion queues.
 */
@Tag("S3Unit")
public class RouterInV2Test {

    /**
     * Given reads in one ordering stripe complete out of order, the queue must release them in arrival order.
     */
    @Test
    public void testOrderQueuePreservesArrivalOrder() {
        RouterInV2.OrderQueue queue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest first = request((short) 1);
        RouterInV2.PartitionProduceRequest second = request((short) 1);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        queue.add(first);
        queue.add(second);

        second.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        queue.drain(drained::add);
        assertEquals(List.of(), drained);

        first.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        queue.drain(drained::add);
        assertEquals(List.of(first, second), drained);
    }

    /**
     * Given the head read fails, the completed failure must not prevent later requests from being released.
     */
    @Test
    public void testOrderQueueAdvancesAfterReadFailure() {
        RouterInV2.OrderQueue queue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest failed = request((short) 1);
        RouterInV2.PartitionProduceRequest succeeded = request((short) 1);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        queue.add(failed);
        queue.add(succeeded);

        succeeded.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        failed.unpackLinkCf.completeExceptionally(new IllegalStateException("read failed"));
        queue.drain(drained::add);

        assertEquals(List.of(failed, succeeded), drained);
    }

    /**
     * Given independent ordering stripes, a completed read must not wait for another stripe's head read.
     */
    @Test
    public void testOrderQueuesDrainIndependently() {
        RouterInV2.OrderQueue blockedQueue = new RouterInV2.OrderQueue();
        RouterInV2.OrderQueue readyQueue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest blocked = request((short) 1);
        RouterInV2.PartitionProduceRequest ready = request((short) 2);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        blockedQueue.add(blocked);
        readyQueue.add(ready);

        ready.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        readyQueue.drain(drained::add);

        assertEquals(List.of(ready), drained);
    }

    /**
     * Given scheduling one completed read fails, the queue must fail that request and continue draining.
     */
    @Test
    public void testOrderQueueContinuesAfterSchedulingFailure() {
        RouterInV2.OrderQueue queue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest failed = request((short) 1);
        RouterInV2.PartitionProduceRequest succeeded = request((short) 1);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        failed.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        succeeded.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        queue.add(failed);
        queue.add(succeeded);

        queue.drain(request -> {
            if (request == failed) {
                throw new IllegalStateException("scheduling failed");
            }
            drained.add(request);
        });

        assertInstanceOf(IllegalStateException.class, failed.responseCf.handle((rst, ex) -> ex).join());
        assertEquals(List.of(succeeded), drained);
    }

    /**
     * Given signed short order hints, indexing must use the complete unsigned 16-bit key space.
     */
    @Test
    public void testOrderHintIndexUsesUnsignedValue() {
        assertEquals(65535 % 3072, RouterInV2.index((short) -1, 3072));
        assertEquals(32768 % 3072, RouterInV2.index(Short.MIN_VALUE, 3072));
    }

    private static RouterInV2.PartitionProduceRequest request(short orderHint) {
        ChannelOffset channelOffset = ChannelOffset.of((short) 0, orderHint, 0, 0, Unpooled.EMPTY_BUFFER);
        RouterInV2.PartitionProduceRequest request = new RouterInV2.PartitionProduceRequest(channelOffset);
        request.unpackLinkCf = new CompletableFuture<>();
        return request;
    }
}
