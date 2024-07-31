/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CollectionHelper {

    public static <T> List<List<T>> groupListByBatchSize(List<T> list, int size) {
        return groupListByBatchSizeAsStream(list, size)
                .collect(Collectors.toList());
    }

    public static <T> Stream<List<T>> groupListByBatchSizeAsStream(List<T> list, int size) {
        if (list == null || size <= 0) {
            throw new IllegalArgumentException("Invalid list or size");
        }

        int listSize = list.size();
        if (listSize <= size) {
            return Stream.of(list);
        }

        return IntStream.range(0, (list.size() + size - 1) / size)
                .mapToObj(i -> list.subList(i * size, Math.min((i + 1) * size, listSize)));
    }

}
