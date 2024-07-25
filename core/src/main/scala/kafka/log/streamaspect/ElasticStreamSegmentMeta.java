/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.storage.internals.log.TimestampOffset;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticStreamSegmentMeta {
    /**
     * base offset of the segment.
     */
    @JsonProperty("bo")
    private long baseOffset;
    @JsonProperty("ct")
    private long createTimestamp;
    @JsonProperty("lmt")
    private long lastModifiedTimestamp;
    @JsonProperty("s")
    private String streamSuffix = "";

    /**
     * size of the log segment. The value is not accurate, it is only used to estimate the size of the segment.
     */
    @JsonProperty("lsz")
    private int logSize;
    /**
     * byte offset range of the log stream
     */
    @JsonProperty("ls")
    private SliceRange log = new SliceRange();
    /**
     * byte offset range of the time index stream
     */
    @JsonProperty("ts")
    private SliceRange time = new SliceRange();
    /**
     * byte offset range of the txn index stream
     */
    @JsonProperty("txs")
    private SliceRange txn = new SliceRange();

    @JsonProperty("fbt")
    private long firstBatchTimestamp;

    @JsonProperty("tle")
    private TimestampOffsetData timeIndexLastEntry = new TimestampOffsetData();

    public ElasticStreamSegmentMeta() {
    }

    public long baseOffset() {
        return baseOffset;
    }

    public void baseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public String streamSuffix() {
        return streamSuffix;
    }

    public void streamSuffix(String streamSuffix) {
        this.streamSuffix = streamSuffix;
    }

    public int logSize() {
        return logSize;
    }

    public void logSize(int logSize) {
        this.logSize = logSize;
    }

    public long createTimestamp() {
        return createTimestamp;
    }

    public void createTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public long lastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public void lastModifiedTimestamp(long lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    public SliceRange log() {
        return log;
    }

    public void log(SliceRange log) {
        this.log = log;
    }

    public SliceRange time() {
        return time;
    }

    public void time(SliceRange time) {
        this.time = time;
    }

    public SliceRange txn() {
        return txn;
    }

    public void txn(SliceRange txn) {
        this.txn = txn;
    }

    public long firstBatchTimestamp() {
        return firstBatchTimestamp;
    }

    public void firstBatchTimestamp(long firstBatchTimestamp) {
        this.firstBatchTimestamp = firstBatchTimestamp;
    }

    public TimestampOffsetData timeIndexLastEntry() {
        return timeIndexLastEntry;
    }

    public void timeIndexLastEntry(TimestampOffsetData timeIndexLastEntry) {
        this.timeIndexLastEntry = timeIndexLastEntry;
    }

    public void timeIndexLastEntry(TimestampOffset timeIndexLastEntry) {
        this.timeIndexLastEntry = TimestampOffsetData.of(timeIndexLastEntry);
    }

    @Override
    public String toString() {
        return "ElasticStreamSegmentMeta{" +
                "baseOffset=" + baseOffset +
                ", createTimestamp=" + createTimestamp +
                ", lastModifiedTimestamp=" + lastModifiedTimestamp +
                ", streamSuffix='" + streamSuffix + '\'' +
                ", logSize=" + logSize +
                ", log=" + log +
                ", time=" + time +
                ", txn=" + txn +
                ", firstBatchTimestamp=" + firstBatchTimestamp +
                ", timeIndexLastEntry=" + timeIndexLastEntry +
                '}';
    }

    public static class TimestampOffsetData {
        @JsonProperty("t")
        private long timestamp;
        @JsonProperty("o")
        private long offset;

        public TimestampOffsetData() {
        }

        public static TimestampOffsetData of(long timestamp, long offset) {
            TimestampOffsetData timestampOffsetData = new TimestampOffsetData();
            timestampOffsetData.timestamp(timestamp);
            timestampOffsetData.offset(offset);
            return timestampOffsetData;
        }

        public static TimestampOffsetData of(TimestampOffset timestampOffset) {
            return of(timestampOffset.indexKey(), timestampOffset.indexValue());
        }

        public TimestampOffset toTimestampOffset() {
            return new TimestampOffset(timestamp, offset);
        }

        public long timestamp() {
            return timestamp;
        }

        public void timestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long offset() {
            return offset;
        }

        public void offset(long offset) {
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "TimestampOffsetData{" +
                    "timestamp=" + timestamp +
                    ", offset=" + offset +
                    '}';
        }
    }

}
