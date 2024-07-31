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

import kafka.autobalancer.model.Snapshot;

public abstract class AbstractSimpleAbnormalMetric implements AbnormalMetric {
    protected final double abnormalThreshold;

    public AbstractSimpleAbnormalMetric(double abnormalThreshold) {
        this.abnormalThreshold = abnormalThreshold;
    }

    @Override
    public boolean isSelfAbnormal(Snapshot self) {
        // TODO: compare with historical data
        return self.getLatest() > abnormalThreshold;
    }
}
