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

package com.automq.stream.s3.wal.common;

import io.netty.buffer.ByteBuf;

public class Record {

    private final ByteBuf header;
    private final ByteBuf body;

    public Record(ByteBuf header, ByteBuf body) {
        this.header = header;
        this.body = body;
    }

    public ByteBuf header() {
        return header;
    }

    public ByteBuf body() {
        return body;
    }
}
