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

package kafka.automq.partition.snapshot;

import java.util.Objects;

public class PartitionSnapshotVersion {
    // increment every time when there is a segment change
    private int segmentsVersion;
    // increment every time when there is a new record
    private int recordsVersion;

    private PartitionSnapshotVersion(int segmentsVersion, int recordsVersion) {
        this.segmentsVersion = segmentsVersion;
        this.recordsVersion = recordsVersion;
    }

    public static PartitionSnapshotVersion create() {
        return new PartitionSnapshotVersion(0, 0);
    }

    public int segmentsVersion() {
        return segmentsVersion;
    }

    public int recordsVersion() {
        return recordsVersion;
    }

    public PartitionSnapshotVersion incrementSegmentsVersion() {
        segmentsVersion++;
        return this;
    }

    public PartitionSnapshotVersion incrementRecordsVersion() {
        recordsVersion++;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        PartitionSnapshotVersion version = (PartitionSnapshotVersion) o;
        return segmentsVersion == version.segmentsVersion && recordsVersion == version.recordsVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentsVersion, recordsVersion);
    }

    public PartitionSnapshotVersion copy() {
        return new PartitionSnapshotVersion(segmentsVersion, recordsVersion);
    }

}
