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

package kafka.log.stream.s3.objects;

import org.apache.kafka.common.message.CommitStreamSetObjectRequestData;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;

public class Convertor {

    public static CommitStreamSetObjectRequestData.StreamObject toStreamObjectInRequest(StreamObject s,
        AutoMQVersion version) {
        CommitStreamSetObjectRequestData.StreamObject obj = new CommitStreamSetObjectRequestData.StreamObject()
            .setStreamId(s.getStreamId())
            .setObjectId(s.getObjectId())
            .setObjectSize(s.getObjectSize())
            .setStartOffset(s.getStartOffset())
            .setEndOffset(s.getEndOffset());
        if (s.getAttributes() == ObjectAttributes.UNSET.attributes()) {
            throw new IllegalArgumentException("[BUG]attributes must be set");
        }
        if (version.isObjectAttributesSupported()) {
            obj.setAttributes(s.getAttributes());
        }
        return obj;
    }

    public static CommitStreamSetObjectRequestData.ObjectStreamRange toObjectStreamRangeInRequest(ObjectStreamRange s) {
        return new CommitStreamSetObjectRequestData.ObjectStreamRange()
            .setStreamId(s.getStreamId())
            .setStartOffset(s.getStartOffset())
            .setEndOffset(s.getEndOffset());
    }
}
