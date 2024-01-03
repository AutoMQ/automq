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

package org.apache.kafka.image;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DeltaMapTest {

    @Test
    public void testCopy() {
        DeltaMap<Integer, Integer> map = new DeltaMap<>(new int[]{2});
        map.putAll(Map.of(1, 1, 2, 2, 3, 3, 5, 5));
        map.removeAll(List.of(3));

        DeltaMap<Integer, Integer> copy1 = map.copy();
        copy1.putAll(Map.of(1, 3));
        assertEquals(1, map.get(1));
        assertEquals(3, copy1.get(1));
        assertNull(map.get(3));
        assertNull(copy1.get(3));

        copy1.putAll(Map.of(4, 4, 1, 11));
        assertEquals(11, copy1.get(1));
        assertEquals(4, copy1.get(4));
    }

    @Test
    public void testDelete() {
        DeltaMap<Integer, Integer> map = new DeltaMap<>(new int[]{10});
        map.putAll(Map.of(1, 1, 2, 2, 3, 3));
        map.removeAll(List.of(3));

        map = map.copy();
        // trigger compact delete
        map.putAll(Map.of(4, 4));

        assertNull(map.get(3));
        assertEquals(0, map.removed.size());
        assertEquals(0, map.deltas.get(0).size());
        assertEquals(3, map.deltas.get(1).size());

        map.removeAll(List.of(4));
        assertEquals(1, map.removed.size());
        assertNull(map.get(4));
        map.putAll(Map.of(4, 44));
        assertEquals(44, map.get(4));
        assertEquals(0, map.removed.size());
    }

}
