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

package kafka.autobalancer.common.types.metrics;

import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.Snapshot;

import java.util.Map;

public interface AbnormalMetric {
    default boolean isAbnormal(Snapshot self, Map<BrokerUpdater.Broker, Snapshot> peers) {
        if (self == null) {
            return false;
        }
        if (isSelfAbnormal(self)) {
            return true;
        }
        return isAbnormalToPeer(self, peers);
    }

    boolean isSelfAbnormal(Snapshot self);

    boolean isAbnormalToPeer(Snapshot self, Map<BrokerUpdater.Broker, Snapshot> peers);
}
