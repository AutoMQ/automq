/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
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
