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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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


}
