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

package org.apache.kafka.metadata.stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class SortedStreamSetObjectsListTest {


    @Test
    public void testSorted() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        objects.add(new S3StreamSetObject(0, -1, Collections.emptyList(), 2));
        objects.add(new S3StreamSetObject(1, -1, Collections.emptyList(), 1));
        objects.add(new S3StreamSetObject(2, -1, Collections.emptyList(), 3));
        objects.add(new S3StreamSetObject(3, -1, Collections.emptyList(), 0));
        objects.add(new S3StreamSetObject(4, -1, Collections.emptyList(), 4));

        assertEquals(5, objects.size());
        List<Long> expectedOrderIds = List.of(0L, 1L, 2L, 3L, 4L);
        assertEquals(expectedOrderIds, objects.list()
            .stream()
            .map(S3StreamSetObject::orderId)
            .collect(Collectors.toList()));

        List<Long> expectedObjectIds = List.of(3L, 1L, 0L, 2L, 4L);
        assertEquals(expectedObjectIds, objects.list()
            .stream()
            .map(S3StreamSetObject::objectId)
            .collect(Collectors.toList()));

        objects.removeIf(obj -> obj.objectId() == 2 || obj.objectId() == 3);

        assertEquals(3, objects.size());
        expectedOrderIds = List.of(1L, 2L, 4L);
        assertEquals(expectedOrderIds, objects.list()
            .stream()
            .map(S3StreamSetObject::orderId)
            .collect(Collectors.toList()));

        expectedObjectIds = List.of(1L, 0L, 4L);
        assertEquals(expectedObjectIds, objects.list()
            .stream()
            .map(S3StreamSetObject::objectId)
            .collect(Collectors.toList()));
    }

    @Test
    public void testDuplicateOrderIds() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        // Add multiple objects with the same orderId but different objectIds
        objects.add(new S3StreamSetObject(100, -1, Collections.emptyList(), 5));
        objects.add(new S3StreamSetObject(101, -1, Collections.emptyList(), 5));
        objects.add(new S3StreamSetObject(102, -1, Collections.emptyList(), 5));
        objects.add(new S3StreamSetObject(200, -1, Collections.emptyList(), 3));
        objects.add(new S3StreamSetObject(300, -1, Collections.emptyList(), 7));

        assertEquals(5, objects.size());
        
        // Verify all objects with orderId=5 are grouped together
        List<Long> orderIds = objects.list().stream()
            .map(S3StreamSetObject::orderId)
            .collect(Collectors.toList());
        assertEquals(List.of(3L, 5L, 5L, 5L, 7L), orderIds);

        // Test contains with duplicate orderIds
        assertEquals(true, objects.contains(new S3StreamSetObject(101, -1, Collections.emptyList(), 5)));
        assertEquals(false, objects.contains(new S3StreamSetObject(999, -1, Collections.emptyList(), 5)));

        // Test remove with duplicate orderIds - should only remove the exact match
        boolean removed = objects.remove(new S3StreamSetObject(101, -1, Collections.emptyList(), 5));
        assertEquals(true, removed);
        assertEquals(4, objects.size());
        
        // Verify the correct object was removed (objectId 101 should be gone)
        List<Long> objectIds = objects.list().stream()
            .map(S3StreamSetObject::objectId)
            .collect(Collectors.toList());
        assertEquals(false, objectIds.contains(101L));
        assertEquals(true, objectIds.contains(100L));
        assertEquals(true, objectIds.contains(102L));
        assertEquals(List.of(200L, 300L), List.of(objectIds.get(0), objectIds.get(3)));
    }

    @Test
    public void testRemoveNonExistent() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        objects.add(new S3StreamSetObject(1, -1, Collections.emptyList(), 10));
        objects.add(new S3StreamSetObject(2, -1, Collections.emptyList(), 20));

        // Try to remove object that doesn't exist
        boolean removed = objects.remove(new S3StreamSetObject(999, -1, Collections.emptyList(), 15));
        assertEquals(false, removed);
        assertEquals(2, objects.size());
    }

    @Test
    public void testEmptyList() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        assertEquals(0, objects.size());
        assertEquals(true, objects.isEmpty());
        assertEquals(false, objects.contains(new S3StreamSetObject(1, -1, Collections.emptyList(), 1)));
    }

    @Test
    public void testSingleElement() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        S3StreamSetObject obj = new S3StreamSetObject(42, -1, Collections.emptyList(), 100);
        
        objects.add(obj);
        assertEquals(1, objects.size());
        assertEquals(true, objects.contains(obj));
        
        objects.remove(obj);
        assertEquals(0, objects.size());
        assertEquals(true, objects.isEmpty());
    }


}
