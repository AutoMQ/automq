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

package kafka.log.s3;

import java.util.List;
import java.util.Queue;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamObjectMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
class StreamObjectsCompactionTaskTest {
    private ObjectManager objectManager;
    private S3Operator s3Operator;
    private S3Stream stream;

    @BeforeEach
    void setUp() {
        objectManager = Mockito.mock(ObjectManager.class);
        s3Operator = Mockito.mock(S3Operator.class);
        stream = Mockito.mock(S3Stream.class);
        Mockito.when(stream.streamId()).thenReturn(1L);
        Mockito.when(stream.startOffset()).thenReturn(5L);
        Mockito.when(stream.nextOffset()).thenReturn(100L);
    }

    @Test
    void prepareCompactGroups() {
        // check if we can filter groups without limit of timestamp
        StreamObjectsCompactionTask task1 = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 100, 0, x -> false);

        long currentTimestamp = System.currentTimeMillis();
        Mockito.when(objectManager.getStreamObjects(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt()))
            .thenReturn(List.of(
                new S3StreamObjectMetadata(new S3StreamObject(1, 150, 1, 5, 10), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(2, 20, 1, 10, 20), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 65, 70), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp)
            ));
        Queue<List<S3StreamObjectMetadata>> compactGroups = task1.prepareCompactGroups(0);
        assertEquals(2, compactGroups.size());

        assertEquals(List.of(new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), currentTimestamp),
            new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), currentTimestamp)), compactGroups.poll());
        assertEquals(List.of(new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 65, 70), currentTimestamp),
            new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp)), compactGroups.poll());
        assertEquals(10, task1.getNextStartSearchingOffset());

        // check if we can filter two groups with limit of timestamp
        StreamObjectsCompactionTask task2 = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 100, 10000, x -> false);

        currentTimestamp = System.currentTimeMillis();
        Mockito.when(objectManager.getStreamObjects(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt()))
            .thenReturn(List.of(
                new S3StreamObjectMetadata(new S3StreamObject(1, 60, 1, 5, 10), currentTimestamp - 20000),
                new S3StreamObjectMetadata(new S3StreamObject(2, 20, 1, 10, 40), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), currentTimestamp - 20000),
                new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 60, 70), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(7, 20, 1, 80, 90), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(8, 20, 1, 90, 99), currentTimestamp)
            ));
        compactGroups = task2.prepareCompactGroups(0);
        assertEquals(1, compactGroups.size());

        assertEquals(List.of(new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 60, 70), currentTimestamp - 30000),
            new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp - 30000),
            new S3StreamObjectMetadata(new S3StreamObject(7, 20, 1, 80, 90), currentTimestamp - 30000)), compactGroups.poll());
        assertEquals(5, task2.getNextStartSearchingOffset());
    }
}