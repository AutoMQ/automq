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

package kafka.log.es;

public class ElasticStreamSegmentMeta {
    private long segmentBaseOffset;
    private long segmentEndOffset = -1L;
    private long logStreamStartOffset;
    private long offsetStreamStartOffset;
    private long timeStreamStartOffset;
    private long txnStreamStartOffset;

    public ElasticStreamSegmentMeta() {
    }

    public long getSegmentBaseOffset() {
        return segmentBaseOffset;
    }

    public void setSegmentBaseOffset(long segmentBaseOffset) {
        this.segmentBaseOffset = segmentBaseOffset;
    }

    public long getLogStreamStartOffset() {
        return logStreamStartOffset;
    }

    public void setLogStreamStartOffset(long logStreamStartOffset) {
        this.logStreamStartOffset = logStreamStartOffset;
    }

    public long getTimeStreamStartOffset() {
        return timeStreamStartOffset;
    }

    public void setTimeStreamStartOffset(long timeStreamStartOffset) {
        this.timeStreamStartOffset = timeStreamStartOffset;
    }

    public long getTxnStreamStartOffset() {
        return txnStreamStartOffset;
    }

    public void setTxnStreamStartOffset(long txnStreamStartOffset) {
        this.txnStreamStartOffset = txnStreamStartOffset;
    }

    public long getSegmentEndOffset() {
        return segmentEndOffset;
    }

    public void setSegmentEndOffset(long segmentEndOffset) {
        this.segmentEndOffset = segmentEndOffset;
    }

    public long getOffsetStreamStartOffset() {
        return offsetStreamStartOffset;
    }

    public void setOffsetStreamStartOffset(long offsetStreamStartOffset) {
        this.offsetStreamStartOffset = offsetStreamStartOffset;
    }
}
