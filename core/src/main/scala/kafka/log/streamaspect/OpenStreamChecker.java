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

package kafka.log.streamaspect;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.s3.StreamFencedException;

/**
 * Check whether a stream is ready for open.
 */
public interface OpenStreamChecker {
    OpenStreamChecker NOOP = (topicId, partition, streamId, epoch) -> true;

    /**
     * Check whether a stream is ready for open.
     */
    boolean check(Uuid topicId, int partition, long streamId, long epoch) throws StreamFencedException;

}
