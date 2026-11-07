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

package org.apache.kafka.image;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeltaListTest {

    @Test
    public void test() {
        DeltaList<Integer> dl = new DeltaList<>();
        dl.add(1);
        dl.add(2);
        dl.add(3);
        DeltaList<Integer> dl2 = dl.copy();
        dl2.add(4);
        dl2.remove(o -> o == 3);
        dl2.add(5);
        dl2.remove(o -> o == 4);
        DeltaList<Integer> dl3 = dl2.copy();
        dl3.add(3);
        dl3.add(6);

        assertEquals(List.of(1, 2, 3), dl.toList());
        assertEquals(List.of(1, 2, 5), dl2.toList());
        assertEquals(List.of(1, 2, 5, 3, 6), dl3.toList());

        dl3.remove(o -> o == 1);
        dl3.remove(o -> o == 2);
        assertEquals(7, dl3.operations.size());
        assertEquals(7, dl3.snapshotIndex);
        assertEquals(2, dl3.dirtyCount);
        DeltaList<Integer> dl4 = dl3.copy();
        assertEquals(List.of(5, 3, 6), dl3.toList());
        // verify the copy is compacted
        assertEquals(3, dl4.operations.size());
        assertEquals(3, dl4.snapshotIndex);
        assertEquals(0, dl4.dirtyCount);
    }
}
