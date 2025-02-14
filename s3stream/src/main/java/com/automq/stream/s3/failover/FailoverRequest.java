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

package com.automq.stream.s3.failover;

public class FailoverRequest {
    private int nodeId;
    private long nodeEpoch;
    private String volumeId;
    private String device;

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getNodeEpoch() {
        return nodeEpoch;
    }

    public void setNodeEpoch(long nodeEpoch) {
        this.nodeEpoch = nodeEpoch;
    }

    public String getVolumeId() {
        return volumeId;
    }

    public void setVolumeId(String volumeId) {
        this.volumeId = volumeId;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    @Override
    public String toString() {
        return "FailoverRequest{" +
            "nodeId=" + nodeId +
            ", nodeEpoch=" + nodeEpoch +
            ", volumeId='" + volumeId + '\'' +
            ", device='" + device + '\'' +
            '}';
    }
}
