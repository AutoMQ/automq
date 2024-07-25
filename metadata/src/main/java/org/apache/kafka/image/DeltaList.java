/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.image;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DeltaList<T> {
    @SuppressWarnings("rawtypes") private static final DeltaList EMPTY_LIST = new DeltaList();
    private static final double MAX_DIRTY_RATIO = 0.2;
    final List<Operation<T>> operations;
    int snapshotIndex;
    int dirtyCount;

    private DeltaList(List<Operation<T>> operations, int dirtyCount) {
        this.operations = operations;
        this.snapshotIndex = operations.size();
        this.dirtyCount = dirtyCount;
    }

    public DeltaList() {
        this.operations = new ArrayList<>();
        this.snapshotIndex = 0;
        this.dirtyCount = 0;
    }

    public DeltaList(List<T> src) {
        this(src.stream().map(Operation::add).collect(Collectors.toList()), 0);
    }

    @SuppressWarnings("unchecked")
    public static <T> DeltaList<T> empty() {
        return (DeltaList<T>) EMPTY_LIST;
    }

    @SafeVarargs
    public static <T> DeltaList<T> of(T... elements) {
        List<Operation<T>> list = new ArrayList<>(elements.length);
        for (T element : elements) {
            list.add(Operation.add(element));
        }
        return new DeltaList<>(list, 0);
    }

    public void add(T t) {
        synchronized (operations) {
            operations.add(Operation.add(t));
            snapshotIndex++;
        }
    }

    public void remove(Matcher<T> matcher) {
        synchronized (operations) {
            operations.add(Operation.remove(matcher));
            snapshotIndex++;
            dirtyCount++;
        }
    }

    public DeltaList<T> copy() {
        synchronized (operations) {
            if (this == EMPTY_LIST) {
                return new DeltaList<>(new ArrayList<>(), 0);
            }
            if (operations.isEmpty()) {
                return new DeltaList<>(operations, dirtyCount);
            }
            int maxDirtyCount = (int) (snapshotIndex * MAX_DIRTY_RATIO);
            if (dirtyCount <= maxDirtyCount) {
                return new DeltaList<>(operations, dirtyCount);
            }
            List<Operation<T>> newOperations = new ArrayList<>(operations.size());
            List<Operation<T>> removedList = new ArrayList<>();
            for (int i = snapshotIndex - 1; i >= 0; i--) {
                Operation<T> operation = operations.get(i);
                if (operation.tombstoneMatcher != null) {
                    removedList.add(operation);
                } else {
                    if (!isRemoved(operation, removedList)) {
                        newOperations.add(operation);
                    }
                }
            }
            Collections.reverse(newOperations);
            return new DeltaList<>(newOperations, 0);
        }
    }

    public void reverseForEach(Consumer<T> consumer) {
        synchronized (operations) {
            List<Operation<T>> removedList = new ArrayList<>();
            for (int i = snapshotIndex - 1; i >= 0; i--) {
                Operation<T> operation = operations.get(i);
                if (operation.tombstoneMatcher != null) {
                    removedList.add(operation);
                } else {
                    if (!isRemoved(operation, removedList)) {
                        consumer.accept(operation.t);
                    }
                }
            }
        }
    }

    public void forEach(Consumer<T> consumer) {
        synchronized (operations) {
            List<Operation<T>> removedList = new ArrayList<>();
            for (int i = 0; i < snapshotIndex; i++) {
                Operation<T> operation = operations.get(i);
                if (operation.tombstoneMatcher != null) {
                    removedList.add(operation);
                }
            }
            for (int i = 0; i < snapshotIndex; i++) {
                Operation<T> operation = operations.get(i);
                if (operation.tombstoneMatcher == null) {
                    if (!isRemoved(operation, removedList)) {
                        consumer.accept(operation.t);
                    }
                }
            }
        }
    }

    public List<T> toList() {
        List<T> list = new ArrayList<>();
        reverseForEach(list::add);
        Collections.reverse(list);
        return list;
    }

    private static <T> boolean isRemoved(Operation<T> operation, List<Operation<T>> removedList) {
        for (Operation<T> removed : removedList) {
            if (removed.tombstoneMatcher.match(operation.t)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DeltaList<?> list = (DeltaList<?>) o;
        return Objects.equals(toList(), list.toList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toList());
    }

    @Override
    public String toString() {
        return toList().toString();
    }

    static class Operation<T> {
        final T t;
        final Matcher<T> tombstoneMatcher;

        public Operation(T t, Matcher<T> tombstoneMatcher) {
            this.t = t;
            this.tombstoneMatcher = tombstoneMatcher;
        }

        public static <T> Operation<T> add(T t) {
            return new Operation<>(t, null);
        }

        public static <T> Operation<T> remove(Matcher<T> matcher) {
            return new Operation<>(null, matcher);
        }
    }

    public interface Matcher<T> {
        boolean match(T t);
    }
}
