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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    public void testBinarySearchAdd() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        
        // Test adding to empty list
        objects.add(new S3StreamSetObject(100, -1, Collections.emptyList(), 50));
        assertEquals(1, objects.size());
        assertEquals(50L, objects.get(0).orderId());
        
        // Test adding at the beginning
        objects.add(new S3StreamSetObject(101, -1, Collections.emptyList(), 10));
        assertEquals(2, objects.size());
        assertEquals(10L, objects.get(0).orderId());
        assertEquals(50L, objects.get(1).orderId());
        
        // Test adding at the end
        objects.add(new S3StreamSetObject(102, -1, Collections.emptyList(), 100));
        assertEquals(3, objects.size());
        assertEquals(100L, objects.get(2).orderId());
        
        // Test adding in the middle
        objects.add(new S3StreamSetObject(103, -1, Collections.emptyList(), 30));
        assertEquals(4, objects.size());
        List<Long> expectedOrderIds = List.of(10L, 30L, 50L, 100L);
        assertEquals(expectedOrderIds, objects.list()
            .stream()
            .map(S3StreamSetObject::orderId)
            .collect(Collectors.toList()));
    }

    @Test
    public void testBinarySearchAddDuplicateOrderId() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        
        // Add objects with the same orderId but different objectIds
        objects.add(new S3StreamSetObject(1, -1, Collections.emptyList(), 50));
        objects.add(new S3StreamSetObject(2, -1, Collections.emptyList(), 50));
        objects.add(new S3StreamSetObject(3, -1, Collections.emptyList(), 50));
        
        assertEquals(3, objects.size());
        // All should have orderId 50
        for (S3StreamSetObject obj : objects) {
            assertEquals(50L, obj.orderId());
        }
    }

    @Test
    public void testBinarySearchRemove() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        objects.add(new S3StreamSetObject(1, -1, Collections.emptyList(), 10));
        objects.add(new S3StreamSetObject(2, -1, Collections.emptyList(), 20));
        objects.add(new S3StreamSetObject(3, -1, Collections.emptyList(), 30));
        objects.add(new S3StreamSetObject(4, -1, Collections.emptyList(), 40));
        objects.add(new S3StreamSetObject(5, -1, Collections.emptyList(), 50));
        
        assertEquals(5, objects.size());
        
        // Remove from middle
        S3StreamSetObject toRemove = new S3StreamSetObject(3, -1, Collections.emptyList(), 30);
        assertTrue(objects.remove(toRemove));
        assertEquals(4, objects.size());
        
        // Verify the element was removed
        List<Long> expectedObjectIds = List.of(1L, 2L, 4L, 5L);
        assertEquals(expectedObjectIds, objects.list()
            .stream()
            .map(S3StreamSetObject::objectId)
            .collect(Collectors.toList()));
        
        // Remove from beginning
        toRemove = new S3StreamSetObject(1, -1, Collections.emptyList(), 10);
        assertTrue(objects.remove(toRemove));
        assertEquals(3, objects.size());
        
        // Remove from end
        toRemove = new S3StreamSetObject(5, -1, Collections.emptyList(), 50);
        assertTrue(objects.remove(toRemove));
        assertEquals(2, objects.size());
        
        // Try to remove non-existent element
        toRemove = new S3StreamSetObject(99, -1, Collections.emptyList(), 999);
        assertFalse(objects.remove(toRemove));
        assertEquals(2, objects.size());
    }

    @Test
    public void testBinarySearchRemoveWithDuplicateOrderIds() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        
        // Add objects with the same orderId but different objectIds
        objects.add(new S3StreamSetObject(1, -1, Collections.emptyList(), 50));
        objects.add(new S3StreamSetObject(2, -1, Collections.emptyList(), 50));
        objects.add(new S3StreamSetObject(3, -1, Collections.emptyList(), 50));
        
        assertEquals(3, objects.size());
        
        // Remove the one with objectId=2
        S3StreamSetObject toRemove = new S3StreamSetObject(2, -1, Collections.emptyList(), 50);
        assertTrue(objects.remove(toRemove));
        assertEquals(2, objects.size());
        
        // Verify remaining elements have objectIds 1 and 3
        List<Long> remainingObjectIds = objects.list()
            .stream()
            .map(S3StreamSetObject::objectId)
            .sorted()
            .collect(Collectors.toList());
        assertEquals(List.of(1L, 3L), remainingObjectIds);
    }

    @Test
    public void testConstructorWithLinkedList() {
        // Verify that constructor properly converts non-ArrayList to ArrayList
        List<S3StreamSetObject> linkedList = new java.util.LinkedList<>();
        linkedList.add(new S3StreamSetObject(1, -1, Collections.emptyList(), 10));
        linkedList.add(new S3StreamSetObject(2, -1, Collections.emptyList(), 20));
        
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList(linkedList);
        assertEquals(2, objects.size());
        
        // Should still work with binary search operations
        objects.add(new S3StreamSetObject(3, -1, Collections.emptyList(), 15));
        assertEquals(3, objects.size());
        
        List<Long> expectedOrderIds = List.of(10L, 15L, 20L);
        assertEquals(expectedOrderIds, objects.list()
            .stream()
            .map(S3StreamSetObject::orderId)
            .collect(Collectors.toList()));
    }

    @Test
    public void testLargeListPerformance() {
        SortedStreamSetObjects objects = new SortedStreamSetObjectsList();
        
        // Add 1000 elements in random order
        List<Long> orderIds = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            orderIds.add(i);
        }
        Collections.shuffle(orderIds);
        
        for (int i = 0; i < orderIds.size(); i++) {
            objects.add(new S3StreamSetObject(i, -1, Collections.emptyList(), orderIds.get(i)));
        }
        
        assertEquals(1000, objects.size());
        
        // Verify the list is sorted
        long prevOrderId = -1;
        for (S3StreamSetObject obj : objects) {
            assertTrue(obj.orderId() > prevOrderId, 
                "List should be sorted by orderId, but found " + obj.orderId() + " after " + prevOrderId);
            prevOrderId = obj.orderId();
        }
    }


}
