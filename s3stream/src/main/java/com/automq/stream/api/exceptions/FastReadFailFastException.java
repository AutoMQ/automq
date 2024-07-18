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
 * Fail fast exception when fast read is enabled and read need read from S3.
 */
public class FastReadFailFastException extends StreamClientException {
    public FastReadFailFastException() {
        super(ErrorCode.FAST_READ_FAIL_FAST, "", false);
    }
}
