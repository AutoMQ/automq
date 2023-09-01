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

package kafka.log.s3.objects;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class CommitWALObjectRequest {
    private long objectId;
    private long objectSize;
    /**
     * The stream ranges of the compacted object.
     */
    private List<ObjectStreamRange> streamRanges;

    /**
     * The stream objects which split from the compacted object.
     */
    private List<StreamObject> streamObjects;
    private long orderId;

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public List<ObjectStreamRange> getStreamRanges() {
        if (streamRanges == null) {
            return Collections.emptyList();
        }
        return streamRanges;
    }

    public void setStreamRanges(List<ObjectStreamRange> streamRanges) {
        this.streamRanges = streamRanges;
    }

    public void addStreamRange(ObjectStreamRange streamRange) {
        if (streamRanges == null) {
            streamRanges = new LinkedList<>();
        }
        streamRanges.add(streamRange);
    }

    public List<StreamObject> getStreamObjects() {
        if (streamObjects == null) {
            return Collections.emptyList();
        }
        return streamObjects;
    }

    public void setStreamObjects(List<StreamObject> streamObjects) {
        this.streamObjects = streamObjects;
    }

    public void addStreamObject(StreamObject streamObject) {
        if (streamObjects == null) {
            streamObjects = new LinkedList<>();
        }
        streamObjects.add(streamObject);
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }
}
