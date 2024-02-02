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

package kafka.autobalancer.common.types;

import java.util.Set;

public class RawMetricTypes {
    public static final byte TOPIC_PARTITION_BYTES_IN = (byte) 0;
    public static final byte TOPIC_PARTITION_BYTES_OUT = (byte) 1;
    public static final byte PARTITION_SIZE = (byte) 2;

    public static Set<Byte> partitionMetrics() {
        return Set.of(TOPIC_PARTITION_BYTES_IN, TOPIC_PARTITION_BYTES_OUT, PARTITION_SIZE);
    }
}
