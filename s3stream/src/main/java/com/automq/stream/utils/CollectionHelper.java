/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
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

    public static <T> List<List<T>> partitionList(List<T> list, int size) {
        return partitionListAsStream(list, size)
                .collect(Collectors.toList());
    }

    public static <T> Stream<List<T>> partitionListAsStream(List<T> list, int size) {
        if (list == null || size <= 0) {
            throw new IllegalArgumentException("Invalid list or size");
        }

        if (list.isEmpty()) {
            return Stream.empty();
        }

        return IntStream.range(0, (list.size() + size - 1) / size)
                .mapToObj(i -> list.subList(i * size, Math.min((i + 1) * size, list.size())));
    }

}
