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

package kafka.cluster;

import org.apache.kafka.storage.internals.log.LogSegment;

public interface LogEventListener {

    void onChanged(LogSegment segment, Event event);

    enum Event {
        SEGMENT_CREATE,
        SEGMENT_DELETE
    }

}
