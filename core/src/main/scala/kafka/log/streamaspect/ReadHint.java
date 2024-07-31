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

import java.util.concurrent.atomic.AtomicBoolean;

public class ReadHint {

    public static final FastThreadLocal<AtomicBoolean> READ_ALL = new FastThreadLocal<>() {
        @Override
        protected AtomicBoolean initialValue() {
            return new AtomicBoolean(false);
        }
    };


    public static final FastThreadLocal<AtomicBoolean> FAST_READ = new FastThreadLocal<>() {
        @Override
        protected AtomicBoolean initialValue() {
            return new AtomicBoolean(false);
        }
    };

    public static boolean isReadAll() {
        return READ_ALL.get().get();
    }

    public static void markReadAll() {
        READ_ALL.get().set(true);
    }

    public static boolean isFastRead() {
        return FAST_READ.get().get();
    }

    public static void markFastRead() {
        FAST_READ.get().set(true);
    }

    public static void clear() {
        READ_ALL.get().set(false);
        FAST_READ.get().set(false);
    }

}
