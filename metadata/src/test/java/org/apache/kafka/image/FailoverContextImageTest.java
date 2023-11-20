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

import org.apache.kafka.common.metadata.FailoverContextRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.FailoverStatus;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FailoverContextImageTest {
    final static FailoverContextImage IMAGE1;
    final static List<ApiMessageAndVersion> DELTA1_RECORDS = new ArrayList<>();
    final static FailoverContextDelta DELTA1;
    final static FailoverContextImage IMAGE2;

    static {
        {
            FailoverContextRecord r1 = new FailoverContextRecord();
            r1.setFailedNodeId(1);
            r1.setStatus(FailoverStatus.WAITING.name());
            FailoverContextRecord r2 = new FailoverContextRecord();
            r2.setFailedNodeId(2);
            r2.setStatus(FailoverStatus.RECOVERING.name());
            r2.setTargetNodeId(233);
            Map<Integer, FailoverContextRecord> map = Map.of(1, r1, 2, r2);
            IMAGE1 = new FailoverContextImage(map);
        }

        {
            FailoverContextRecord r1 = new FailoverContextRecord();
            r1.setFailedNodeId(1);
            r1.setStatus(FailoverStatus.RECOVERING.name());
            r1.setTargetNodeId(234);
            FailoverContextRecord r2 = new FailoverContextRecord();
            r2.setFailedNodeId(2);
            r2.setStatus(FailoverStatus.DONE.name());
            r2.setTargetNodeId(233);
            Map<Integer, FailoverContextRecord> map = Map.of(1, r1, 2, r2);
            DELTA1_RECORDS.add(new ApiMessageAndVersion(r1, (short) 0));
            DELTA1_RECORDS.add(new ApiMessageAndVersion(r2, (short) 0));
            DELTA1 = new FailoverContextDelta(IMAGE1);
            RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);
        }

        {
            FailoverContextRecord r1 = new FailoverContextRecord();
            r1.setFailedNodeId(1);
            r1.setStatus(FailoverStatus.RECOVERING.name());
            r1.setTargetNodeId(234);
            Map<Integer, FailoverContextRecord> map = Map.of(1, r1);
            IMAGE2 = new FailoverContextImage(map);
        }
    }

    @Test
    public void testImage1RoundTrip() {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    private void testToImageAndBack(FailoverContextImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        FailoverContextDelta delta = new FailoverContextDelta(FailoverContextImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        FailoverContextImage newImage = delta.apply();
        assertEquals(image, newImage);
    }
}