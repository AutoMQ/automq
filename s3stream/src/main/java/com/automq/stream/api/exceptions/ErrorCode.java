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

package com.automq.stream.api.exceptions;

public class ErrorCode {

    public static final short UNEXPECTED = 99;

    public static final short STREAM_ALREADY_CLOSED = 1;
    public static final short STREAM_NOT_EXIST = 2;
    public static final short EXPIRED_STREAM_EPOCH = 3;
    public static final short STREAM_NOT_CLOSED = 4;

    public static final short OFFSET_OUT_OF_RANGE_BOUNDS = 10;
    public static final short FAST_READ_FAIL_FAST = 11;

}
