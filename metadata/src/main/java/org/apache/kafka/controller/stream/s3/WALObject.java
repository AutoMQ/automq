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

package org.apache.kafka.controller.stream.s3;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WALObject extends S3Object {
    private Integer brokerId;
    private Map<Long/*streamId*/, ObjectStreamIndex> streamsIndex;

    private S3ObjectType objectType = S3ObjectType.UNKNOWN;

    public WALObject(Long objectId) {
        super(objectId);
    }

    @Override
    public void onCreate(S3ObjectCreateContext createContext) {
        super.onCreate(createContext);
        WALObjectCreateContext walCreateContext = (WALObjectCreateContext) createContext;
        this.streamsIndex = walCreateContext.streamIndexList.stream().collect(Collectors.toMap(ObjectStreamIndex::getStreamId, index -> index));
        this.brokerId = walCreateContext.brokerId;
    }

    class WALObjectCreateContext extends S3ObjectCreateContext {

        private final List<ObjectStreamIndex> streamIndexList;
        private final Integer brokerId;

        public WALObjectCreateContext(
            final Long createTimeInMs,
            final Long objectSize,
            final String objectAddress,
            final S3ObjectType objectType,
            final List<ObjectStreamIndex> streamIndexList,
            final Integer brokerId) {
            super(createTimeInMs, objectSize, objectAddress, objectType);
            this.streamIndexList = streamIndexList;
            this.brokerId = brokerId;
        }
    }
}
