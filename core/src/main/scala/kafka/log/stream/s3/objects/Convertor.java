/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.objects;

import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData;

public class Convertor {

    public static CommitStreamSetObjectRequestData.StreamObject toStreamObjectInRequest(StreamObject s) {
        return new CommitStreamSetObjectRequestData.StreamObject()
                .setStreamId(s.getStreamId())
                .setObjectId(s.getObjectId())
                .setObjectSize(s.getObjectSize())
                .setStartOffset(s.getStartOffset())
                .setEndOffset(s.getEndOffset());
    }

    public static CommitStreamSetObjectRequestData.ObjectStreamRange toObjectStreamRangeInRequest(ObjectStreamRange s) {
        return new CommitStreamSetObjectRequestData.ObjectStreamRange()
                .setStreamId(s.getStreamId())
                .setStartOffset(s.getStartOffset())
                .setEndOffset(s.getEndOffset());
    }
}
