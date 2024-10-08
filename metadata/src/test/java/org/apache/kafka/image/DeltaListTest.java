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
