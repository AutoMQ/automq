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

package kafka.log.stream.s3.objects;

import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import org.apache.kafka.common.message.CommitWALObjectRequestData;

public class Convertor {

    public static CommitWALObjectRequestData.StreamObject toStreamObjectInRequest(StreamObject s) {
        return new CommitWALObjectRequestData.StreamObject()
                .setStreamId(s.getStreamId())
                .setObjectId(s.getObjectId())
                .setObjectSize(s.getObjectSize())
                .setStartOffset(s.getStartOffset())
                .setEndOffset(s.getEndOffset());
    }

    public static CommitWALObjectRequestData.ObjectStreamRange toObjectStreamRangeInRequest(ObjectStreamRange s) {
        return new CommitWALObjectRequestData.ObjectStreamRange()
                .setStreamId(s.getStreamId())
                .setStartOffset(s.getStartOffset())
                .setEndOffset(s.getEndOffset());
    }
}
