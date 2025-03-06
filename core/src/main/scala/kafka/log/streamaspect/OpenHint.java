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

import io.netty.util.concurrent.FastThreadLocal;

public class OpenHint {
    public static final FastThreadLocal<Boolean> SNAPSHOT_READ = new FastThreadLocal<>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static void markSnapshotRead() {
        SNAPSHOT_READ.set(true);
    }

    public static boolean isSnapshotRead() {
        return SNAPSHOT_READ.get();
    }

    public static void clear() {
        SNAPSHOT_READ.remove();
    }
}
