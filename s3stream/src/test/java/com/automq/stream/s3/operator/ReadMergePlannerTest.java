/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.operator.ReadRequestAccumulator.MergedReadTask;
import com.automq.stream.s3.operator.ReadRequestAccumulator.ReadTask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Tag("S3Unit")
class ReadMergePlannerTest {
    private static final long BLOCK = 128L * 1024;
    private static final long MAX_READ = 4L * 1024 * 1024;
    private final ReadRequestAccumulator.Reader reader = mock(ReadRequestAccumulator.Reader.class);
    private final Logger logger = mock(Logger.class);

    /** Given two sparse reads in one block, planning merges them without aligning the actual GET. */
    @Test
    void mergesWithinBlockUsingExactBounds() {
        List<MergedReadTask> plans = plan(0, task(4096, 8192), task(100 * 1024, 104 * 1024));
        assertEquals(1, plans.size());
        assertEquals(4096, plans.get(0).start);
        assertEquals(104 * 1024, plans.get(0).end);
        assertEquals(2, plans.get(0).readTasks.size());
    }

    /** Given an initially invalid sparse prefix, planning considers the dense suffix before choosing GETs. */
    @Test
    void considersWholeBatchInsteadOfRejectingSparsePrefix() {
        List<ReadTask> tasks = new ArrayList<>();
        tasks.add(task(17, 33));
        for (int i = 5; i < 9; i++) {
            tasks.add(task(i * BLOCK + 17, i * BLOCK + 33));
        }
        List<MergedReadTask> plans = planner(0.5f).plan(tasks);
        assertEquals(1, plans.size());
        assertEquals(17, plans.get(0).start);
        assertEquals(8 * BLOCK + 33, plans.get(0).end);
    }

    /** Given a sparse batch and an oversized individual read, planning enforces the merge cap without splitting callers. */
    @Test
    void preservesSizeLimitAndReadToEnd() {
        List<MergedReadTask> plans = plan(0.5f, task(0, 16), task(4 * BLOCK, 4 * BLOCK + 16),
            task(10 * MAX_READ, 11 * MAX_READ + 1), task(20 * MAX_READ, -1));
        assertEquals(4, plans.size());
        assertTrue(plans.stream().anyMatch(p -> p.end == -1));
        assertTrue(plans.stream().anyMatch(p -> p.start == 10 * MAX_READ && p.end == 11 * MAX_READ + 1));
    }

    /** Given many overlapping and duplicate requests, each caller belongs to exactly one covering GET. */
    @Test
    void preservesEveryRequestWithoutExceedingMergeLimit() {
        Random random = new Random(42);
        List<ReadTask> tasks = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            long start = random.nextInt(8 * 1024 * 1024);
            tasks.add(task(start, start + random.nextInt(512 * 1024) + 1));
        }
        List<MergedReadTask> plans = planner(0.5f).plan(tasks);
        assertEquals(tasks.size(), plans.stream().mapToInt(p -> p.readTasks.size()).sum());
        for (MergedReadTask p : plans) {
            assertTrue(p.end - p.start <= MAX_READ);
            for (ReadTask task : p.readTasks) {
                assertTrue(p.start <= task.start && task.end <= p.end);
            }
        }
        assertEquals(tasks.size(), plans.stream().flatMap(p -> p.readTasks.stream()).distinct().count());
    }

    /** Given small sparse batches, planning matches exhaustive partitions on GET count and exact bytes. */
    @Test
    void matchesExhaustivePlanningForSparseBatches() {
        Random random = new Random(73);
        for (int sample = 0; sample < 100; sample++) {
            List<ReadTask> tasks = new ArrayList<>();
            long block = 0;
            for (int i = 0; i < 8; i++) {
                block += random.nextInt(12) + 1;
                long start = block * BLOCK + random.nextInt(1024);
                tasks.add(task(start, start + random.nextInt(8192) + 1));
            }
            tasks.sort(Comparator.comparingLong(t -> t.start));
            long[] expected = exhaustive(tasks, 0);
            List<MergedReadTask> actual = planner(0.5f).plan(tasks);
            assertEquals(expected[0], actual.size());
            assertEquals(expected[1], actual.stream().mapToLong(p -> p.end - p.start).sum());
        }
    }

    /** Given overlapping anchors across the size cap, early atomic grouping must not increase GET count. */
    @Test
    void keepsCutsInsideOverlappingComponents() {
        List<MergedReadTask> plans = plan(0.5f, task(0, BLOCK), task(12 * BLOCK, 24 * BLOCK),
            task(18 * BLOCK, 44 * BLOCK), task(27 * BLOCK, 48 * BLOCK));
        assertEquals(2, plans.size());
        assertEquals(0, plans.get(0).start);
        assertEquals(24 * BLOCK, plans.get(0).end);
        assertEquals(18 * BLOCK, plans.get(1).start);
        assertEquals(48 * BLOCK, plans.get(1).end);
    }

    /** Given equal GET counts, the planner chooses cuts minimizing actual bytes rather than coarse block bounds. */
    @Test
    void minimizesBytesAcrossOverlappingAnchors() {
        long unit = 32768;
        List<MergedReadTask> plans = plan(0.5f, task(40 * unit, 86 * unit), task(85 * unit, 167 * unit),
            task(120 * unit, 201 * unit));
        assertEquals(2, plans.size());
        assertEquals(162 * unit, plans.stream().mapToLong(p -> p.end - p.start).sum());
    }

    /** Given a maximum-sized GET with unaligned endpoints, all 33 touched blocks remain eligible. */
    @Test
    void acceptsMaximumUnalignedRange() {
        List<MergedReadTask> plans = plan(1, task(17, 33), task(MAX_READ, MAX_READ + 17));
        assertEquals(1, plans.size());
        assertEquals(MAX_READ, plans.get(0).end - plans.get(0).start);
    }

    /** Given overlapping, nested and same-block requests, optimized planning matches exhaustive exact-range partitions. */
    @Test
    void matchesExhaustivePlanningForOverlappingBatches() {
        Random random = new Random(97);
        for (int sample = 0; sample < 100; sample++) {
            List<ReadTask> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                long start = random.nextInt(8 * 1024 * 1024);
                tasks.add(task(start, start + random.nextInt(3 * 1024 * 1024) + 1));
            }
            tasks.sort(Comparator.comparingLong(t -> t.start));
            long[] expected = exhaustive(tasks, 0);
            List<MergedReadTask> actual = planner(0.5f).plan(tasks);
            assertEquals(expected[0], actual.size(), "GET count for sample " + sample);
            assertEquals(expected[1], actual.stream().mapToLong(p -> p.end - p.start).sum(), "Bytes for sample " + sample);
        }
    }

    private long[] exhaustive(List<ReadTask> tasks, int first) {
        if (first == tasks.size()) {
            return new long[] {0, 0};
        }
        long[] best = {Long.MAX_VALUE, Long.MAX_VALUE};
        Set<Long> occupied = new HashSet<>();
        long end = 0;
        for (int last = first; last < tasks.size(); last++) {
            ReadTask task = tasks.get(last);
            end = Math.max(end, task.end);
            for (long block = task.start / BLOCK; block <= (task.end - 1) / BLOCK; block++) {
                occupied.add(block);
            }
            long bytes = end - tasks.get(first).start;
            long span = (end - 1) / BLOCK - tasks.get(first).start / BLOCK + 1;
            if (bytes <= MAX_READ && occupied.size() * 2L >= span) {
                long[] rest = exhaustive(tasks, last + 1);
                long count = rest[0] + 1;
                long total = rest[1] + bytes;
                if (count < best[0] || (count == best[0] && total < best[1])) {
                    best = new long[] {count, total};
                }
            }
        }
        return best;
    }

    private ReadMergePlanner planner(float sparsity) {
        return new ReadMergePlanner(sparsity, (tasks, rate) ->
            new MergedReadTask(tasks, rate, reader, logger, Runnable::run, task -> { }));
    }

    private List<MergedReadTask> plan(float sparsity, ReadTask... tasks) {
        return planner(sparsity).plan(List.of(tasks));
    }

    private ReadTask task(long start, long end) {
        return new ReadTask(new ObjectStorage.ReadOptions(), "key", start, end, new CompletableFuture<>());
    }
}
