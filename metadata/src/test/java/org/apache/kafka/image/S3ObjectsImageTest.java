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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.SimplifiedS3Object;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40)
public class S3ObjectsImageTest {
    final static S3ObjectsImage IMAGE1;

    final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static S3ObjectsDelta DELTA1;

    final static S3ObjectsImage IMAGE2;

    static {
        Map<Long/*objectId*/, SimplifiedS3Object> map = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            SimplifiedS3Object object = new SimplifiedS3Object(i, S3ObjectState.PREPARED);
            map.put(object.objectId(), object);
        }
        IMAGE1 = new S3ObjectsImage(map);
        DELTA1_RECORDS = new ArrayList<>();
        // try to update object0 and object1 to committed
        // try to make object2 expired and mark it to be destroyed
        // try to remove destroy object3
        // try to add applied object4
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(0L).
            setObjectState((byte) S3ObjectState.COMMITTED.ordinal()), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(1L).
            setObjectState((byte) S3ObjectState.COMMITTED.ordinal()), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(2L).
            setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal()), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveS3ObjectRecord()
            .setObjectId(3L), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(4L).
            setObjectState((byte) S3ObjectState.PREPARED.ordinal()), (short) 0));
        DELTA1 = new S3ObjectsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<Long/*objectId*/, SimplifiedS3Object> map2 = new HashMap<>();
        map2.put(0L, new SimplifiedS3Object(0L, S3ObjectState.COMMITTED));
        map2.put(1L, new SimplifiedS3Object(1L, S3ObjectState.COMMITTED));
        map2.put(2L, new SimplifiedS3Object(2L, S3ObjectState.MARK_DESTROYED));
        map2.put(4L, new SimplifiedS3Object(4L, S3ObjectState.PREPARED));

        IMAGE2 = new S3ObjectsImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImageAndBack(S3ObjectsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(S3ObjectsImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        S3ObjectsDelta delta = new S3ObjectsDelta(S3ObjectsImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        S3ObjectsImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

}
