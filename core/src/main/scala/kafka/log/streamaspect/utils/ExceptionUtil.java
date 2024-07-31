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
package kafka.log.streamaspect.utils;

import org.slf4j.Logger;

public final class ExceptionUtil {
    public static void maybeRecordThrowableAndRethrow(Runnable runnable, String message, Logger logger) {
        try {
            runnable.run();
        } catch (Throwable t) {
            logger.error("{} ", message, t);
            throw t;
        }
    }

    public static void maybeRecordThrowableAndRethrow(Runnable runnable, String message, kafka.utils.Logging logger) {
        try {
            runnable.run();
        } catch (Throwable t) {
            logger.error(() -> message, () -> t);
            throw t;
        }
    }
}
