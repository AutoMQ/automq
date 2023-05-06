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

import com.fasterxml.jackson.annotation.JsonProperty;

public class ElasticStreamSegmentMeta {
    @JsonProperty("sbo")
    private long segmentBaseOffset;
    @JsonProperty("sp")
    private String streamSuffix;
    @JsonProperty("lso")
    private long logStreamStartOffset;
    @JsonProperty("leo")
    private long logStreamEndOffset;
    @JsonProperty("oso")
    private long offsetStreamStartOffset;
    @JsonProperty("oeo")
    private long offsetStreamEndOffset;
    @JsonProperty("tso")
    private long timeStreamStartOffset;
    @JsonProperty("teo")
    private long timeStreamEndOffset;
    @JsonProperty("txnso")
    private long txnStreamStartOffset;
    @JsonProperty("txneo")
    private long txnStreamEndOffset;

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

    public long getOffsetStreamStartOffset() {
        return offsetStreamStartOffset;
    }

    public void setOffsetStreamStartOffset(long offsetStreamStartOffset) {
        this.offsetStreamStartOffset = offsetStreamStartOffset;
    }

    public long getLogStreamEndOffset() {
        return logStreamEndOffset;
    }

    public void setLogStreamEndOffset(long logStreamEndOffset) {
        this.logStreamEndOffset = logStreamEndOffset;
    }

    public long getOffsetStreamEndOffset() {
        return offsetStreamEndOffset;
    }

    public void setOffsetStreamEndOffset(long offsetStreamEndOffset) {
        this.offsetStreamEndOffset = offsetStreamEndOffset;
    }

    public long getTimeStreamEndOffset() {
        return timeStreamEndOffset;
    }

    public void setTimeStreamEndOffset(long timeStreamEndOffset) {
        this.timeStreamEndOffset = timeStreamEndOffset;
    }

    public long getTxnStreamEndOffset() {
        return txnStreamEndOffset;
    }

    public void setTxnStreamEndOffset(long txnStreamEndOffset) {
        this.txnStreamEndOffset = txnStreamEndOffset;
    }

    public String getStreamSuffix() {
        return streamSuffix;
    }

    public void setStreamSuffix(String streamSuffix) {
        this.streamSuffix = streamSuffix;
    }

    @Override
    public String toString() {
        return "ElasticStreamSegmentMeta{" +
                "segmentBaseOffset=" + segmentBaseOffset +
                ", streamSuffix='" + streamSuffix + '\'' +
                ", logStreamStartOffset=" + logStreamStartOffset +
                ", logStreamEndOffset=" + logStreamEndOffset +
                ", offsetStreamStartOffset=" + offsetStreamStartOffset +
                ", offsetStreamEndOffset=" + offsetStreamEndOffset +
                ", timeStreamStartOffset=" + timeStreamStartOffset +
                ", timeStreamEndOffset=" + timeStreamEndOffset +
                ", txnStreamStartOffset=" + txnStreamStartOffset +
                ", txnStreamEndOffset=" + txnStreamEndOffset +
                '}';
    }
}
