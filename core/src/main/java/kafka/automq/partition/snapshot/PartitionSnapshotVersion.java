/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.partition.snapshot;

import kafka.log.streamaspect.ElasticLogMeta;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class PartitionSnapshotVersion {
    // increment every time when there is a new record
    private int recordsVersion;
    private final AtomicReference<ElasticLogMeta> logMeta;

    PartitionSnapshotVersion(int recordsVersion, ElasticLogMeta logMeta) {
        this.recordsVersion = recordsVersion;
        this.logMeta = new AtomicReference<>(logMeta);
    }

    public static PartitionSnapshotVersion create() {
        return new PartitionSnapshotVersion(0, null);
    }

    public int recordsVersion() {
        return recordsVersion;
    }

    public PartitionSnapshotVersion incrementRecordsVersion() {
        recordsVersion++;
        return this;
    }

    /** Returns the metadata used for snapshot change detection. */
    public ElasticLogMeta logMeta() {
        return logMeta.get();
    }

    /** Records the latest metadata change notification. */
    public void logMeta(ElasticLogMeta logMeta) {
        this.logMeta.set(logMeta);
    }

    /** Initializes the metadata baseline if no notification has arrived. */
    void initializeLogMeta(ElasticLogMeta logMeta) {
        this.logMeta.compareAndSet(null, logMeta);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PartitionSnapshotVersion version = (PartitionSnapshotVersion) o;
        return recordsVersion == version.recordsVersion && Objects.equals(logMeta(), version.logMeta());
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsVersion, logMeta());
    }

    public PartitionSnapshotVersion copy() {
        return new PartitionSnapshotVersion(recordsVersion, logMeta());
    }

}
