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

package org.apache.kafka.image;

import java.util.Objects;
import org.apache.kafka.common.Uuid;

public class TopicIdPartition {
    final Uuid topicId;
    final int partition;

    TopicIdPartition(Uuid id, int partition) {
        topicId = id;
        this.partition = partition;
    }

    public Uuid topicId() {
        return topicId;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TopicIdPartition partition1 = (TopicIdPartition) o;
        return partition == partition1.partition && Objects.equals(topicId, partition1.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, partition);
    }
}
