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

package org.apache.kafka.clients.admin;

import java.util.Objects;

public class UpdateGroupSpec {
    private String clusterId;
    private boolean promoted;

    public UpdateGroupSpec clusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public UpdateGroupSpec promoted(boolean promoted) {
        this.promoted = promoted;
        return this;
    }

    public String clusterId() {
        return clusterId;
    }

    public boolean promoted() {
        return promoted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        UpdateGroupSpec spec = (UpdateGroupSpec) o;
        return promoted == spec.promoted && Objects.equals(clusterId, spec.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, promoted);
    }

    @Override
    public String toString() {
        return "UpdateGroupsSpec{" +
            "clusterId='" + clusterId + '\'' +
            ", promoted=" + promoted +
            '}';
    }
}
