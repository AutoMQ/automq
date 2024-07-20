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

package com.automq.stream.api.exceptions;

/**
 * All stream client exceptions will list extends StreamClientException and list here.
 */
public class StreamClientException extends RuntimeException {
    private final int code;

    public StreamClientException(int code, String str) {
        this(code, str, null);
    }

    public StreamClientException(int code, String str, Throwable e) {
        super("code: " + code + ", " + str, e);
        this.code = code;
    }

    public StreamClientException(int code, String str, boolean writableStackTrace) {
        super("code: " + code + ", " + str, null, false, writableStackTrace);
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}
