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
