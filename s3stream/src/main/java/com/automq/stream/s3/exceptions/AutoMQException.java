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

package com.automq.stream.s3.exceptions;

public class AutoMQException extends RuntimeException {

    public AutoMQException() {
    }

    public AutoMQException(String message) {
        super(message);
    }

    public AutoMQException(String message, Throwable cause) {
        super(message, cause);
    }

    public AutoMQException(Throwable cause) {
        super(cause);
    }

    public AutoMQException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
