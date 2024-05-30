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

package com.automq.shell.log;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

public class S3RollingFileAppender extends RollingFileAppender {
    private final LogUploader logUploader = LogUploader.getInstance();

    @Override
    protected void subAppend(LoggingEvent event) {
        super.subAppend(event);
        if (!closed) {
            LogRecorder.LogEvent logEvent = new LogRecorder.LogEvent(
                event.getTimeStamp(),
                event.getLevel().toString(),
                event.getLoggerName(),
                event.getRenderedMessage(),
                event.getThrowableStrRep());

            try {
                logEvent.validate();
            } catch (IllegalArgumentException e) {
                // Drop invalid log event
                errorHandler.error("Failed to validate log event", e, 0);
                return;
            }

            logUploader.append(logEvent);
        }
    }
}
