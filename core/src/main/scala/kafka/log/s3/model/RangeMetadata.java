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

package kafka.log.s3.model;


public class RangeMetadata {
    private static final long NOOP_OFFSET = -1;
    private int index;
    private long startOffset;
    private long endOffset;
    private int brokerId;

    public RangeMetadata(int index, long startOffset, long endOffset, int serverId) {
        this.index = index;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.brokerId = serverId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }
}
