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

package org.apache.kafka.shell;

import kafka.utils.Json;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClusterSummary {
    private final Map<Uuid, Set<Integer>> topicPartitionMap = new HashMap<>();
    private final Map<Long, Long> s3ObjectSizeMap = new HashMap<>();
    private int brokerNum;
    private int streamNum;
    private int streamObjectNum;
    private int streamSetObjectNum;

    public Map<Uuid, Set<Integer>> topicPartitionMap() {
        return topicPartitionMap;
    }

    public Map<Long, Long> s3ObjectSizeMap() {
        return s3ObjectSizeMap;
    }

    public int getBrokerNum() {
        return brokerNum;
    }

    public void setBrokerNum(int brokerNum) {
        this.brokerNum = brokerNum;
    }

    public int getStreamNum() {
        return streamNum;
    }

    public void setStreamNum(int streamNum) {
        this.streamNum = streamNum;
    }

    public int getStreamObjectNum() {
        return streamObjectNum;
    }

    public void setStreamObjectNum(int streamObjectNum) {
        this.streamObjectNum = streamObjectNum;
    }

    public int getStreamSetObjectNum() {
        return streamSetObjectNum;
    }

    public void setStreamSetObjectNum(int streamSetObjectNum) {
        this.streamSetObjectNum = streamSetObjectNum;
    }

    @Override
    public String toString() {
        int partitionNum = (int) topicPartitionMap.values().stream().mapToLong(Set::size).sum();
        long s3ObjectSize = s3ObjectSizeMap.values().stream().mapToLong(v -> v).sum();
        return Json.encodeAsString(Map.of(
                "brokerNum", brokerNum,
                "streamNum", streamNum,
                "topicNum", topicPartitionMap.size(),
                "partitionNum", partitionNum,
                "streamObjectNum", streamObjectNum,
                "streamSetObjectNum", streamSetObjectNum,
                "s3ObjectNum", s3ObjectSizeMap.size(),
                "s3ObjectSize", String.format("%.2fMB", (double) s3ObjectSize / (1024 * 1024))));
    }
}
