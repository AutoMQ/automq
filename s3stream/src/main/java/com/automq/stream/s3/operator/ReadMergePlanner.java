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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 * Plans exact GET ranges using 128KiB occupancy intervals (a compressed bitset). Contained callers
 * share a covering anchor. For the remaining ordered anchors, dynamic programming minimizes GET
 * count, then bytes. Sliding minima retain all possible cuts without comparing every request pair:
 * after sorting, at most 33 starting blocks fit in each 4MiB candidate window.
 */
final class ReadMergePlanner {
    static final long BLOCK_SIZE = 128L * 1024;
    static final long MAX_MERGE_READ_SIZE = 4L * 1024 * 1024;
    private final float maxSparsity;
    private final BiFunction<List<ReadTask>, Double, MergedReadTask> taskFactory;

    ReadMergePlanner(float maxSparsity, BiFunction<List<ReadTask>, Double, MergedReadTask> taskFactory) {
        this.maxSparsity = maxSparsity;
        this.taskFactory = taskFactory;
    }

    List<MergedReadTask> plan(List<ReadTask> requests) {
        List<ReadTask> sorted = new ArrayList<>(requests);
        sorted.sort(Comparator.comparingLong((ReadTask t) -> t.start)
            .thenComparing(Comparator.comparingLong((ReadTask t) -> t.end).reversed()));
        List<MergedReadTask> result = new ArrayList<>();
        List<Anchor> anchors = new ArrayList<>();
        for (ReadTask task : sorted) {
            if (task.cf.isDone()) {
                continue;
            }
            if (task.end == ObjectStorage.RANGE_READ_TO_END) {
                result.add(taskFactory.apply(List.of(task), 0.0));
                continue;
            }
            Anchor last = anchors.isEmpty() ? null : anchors.get(anchors.size() - 1);
            if (last != null && task.end <= last.end) {
                last.tasks.add(task);
            } else {
                anchors.add(new Anchor(task));
            }
        }
        // Oversized individual reads remain intact and do not expand the planner's candidate window.
        List<Anchor> bounded = new ArrayList<>();
        for (Anchor anchor : anchors) {
            if (anchor.end - anchor.start > MAX_MERGE_READ_SIZE) {
                planAnchors(bounded, result);
                bounded.clear();
                result.add(taskFactory.apply(anchor.tasks, 0.0));
            } else {
                bounded.add(anchor);
            }
        }
        planAnchors(bounded, result);
        result.sort(Comparator.comparingLong(t -> t.start));
        return result;
    }

    private void planAnchors(List<Anchor> anchors, List<MergedReadTask> result) {
        int n = anchors.size();
        int[] counts = new int[n + 1];
        long[] bytes = new long[n + 1];
        int[] previous = new int[n + 1];
        double[] sparsities = new double[n + 1];
        long[] gaps = new long[n];
        Map<Long, Deque<Integer>> candidates = new TreeMap<>();
        for (int j = 0; j < n; j++) {
            Anchor last = anchors.get(j);
            if (j > 0) {
                gaps[j] = gaps[j - 1] + Math.max(0, last.firstBlock() - anchors.get(j - 1).lastBlock() - 1);
            }
            addCandidate(candidates, j, counts, bytes, anchors);
            counts[j + 1] = Integer.MAX_VALUE;
            Iterator<Deque<Integer>> iterator = candidates.values().iterator();
            while (iterator.hasNext()) {
                Deque<Integer> queue = iterator.next();
                while (!queue.isEmpty() && last.end - anchors.get(queue.peekFirst()).start > MAX_MERGE_READ_SIZE) {
                    queue.removeFirst();
                }
                if (queue.isEmpty()) {
                    iterator.remove();
                    continue;
                }
                int i = queue.peekFirst();
                Anchor first = anchors.get(i);
                // Non-contained anchors have increasing ends. Their coarse union's missing blocks
                // are prefix gap differences, identical for every candidate in one starting block.
                long blocks = last.lastBlock() - first.firstBlock() + 1;
                double sparsity = (double) (gaps[j] - gaps[i]) / blocks;
                if (sparsity > maxSparsity) {
                    continue;
                }
                int candidateCount = counts[i] + 1;
                long candidateBytes = bytes[i] + last.end - first.start;
                if (candidateCount < counts[j + 1]
                    || (candidateCount == counts[j + 1] && candidateBytes < bytes[j + 1])) {
                    counts[j + 1] = candidateCount;
                    bytes[j + 1] = candidateBytes;
                    previous[j + 1] = i;
                    sparsities[j + 1] = sparsity;
                }
            }
        }
        collectPlans(anchors, result, previous, sparsities);
    }

    private void addCandidate(Map<Long, Deque<Integer>> candidates, int index, int[] counts, long[] bytes,
        List<Anchor> anchors) {
        Deque<Integer> queue = candidates.computeIfAbsent(anchors.get(index).firstBlock(), k -> new ArrayDeque<>());
        while (!queue.isEmpty() && betterCandidate(index, queue.peekLast(), counts, bytes, anchors)) {
            queue.removeLast();
        }
        queue.addLast(index);
    }

    private void collectPlans(List<Anchor> anchors, List<MergedReadTask> result, int[] previous, double[] sparsities) {
        for (int j = anchors.size(); j > 0; j = previous[j]) {
            List<ReadTask> tasks = new ArrayList<>();
            for (int i = previous[j]; i < j; i++) {
                tasks.addAll(anchors.get(i).tasks);
            }
            result.add(taskFactory.apply(tasks, sparsities[j]));
        }
    }

    private boolean betterCandidate(int candidate, int previous, int[] counts, long[] bytes, List<Anchor> anchors) {
        return counts[candidate] < counts[previous] || (counts[candidate] == counts[previous]
            && bytes[candidate] - anchors.get(candidate).start <= bytes[previous] - anchors.get(previous).start);
    }

    private static final class Anchor {
        private final List<ReadTask> tasks = new ArrayList<>();
        private final long start;
        private final long end;

        private Anchor(ReadTask task) {
            start = task.start;
            end = task.end;
            tasks.add(task);
        }

        private long firstBlock() {
            return start / BLOCK_SIZE;
        }

        private long lastBlock() {
            return (end - 1) / BLOCK_SIZE;
        }
    }
}
