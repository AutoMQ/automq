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

package com.automq.stream.s3.metadata;

public enum S3ObjectType {
    /**
     * STREAM_SET object which contains multiple streams' records
     */
    STREAM_SET,

    /**
     * STREAM object which only contains one stream's records.
     */
    STREAM,

    /**
     * UNKNOWN object type
     */
    UNKNOWN;

    public static S3ObjectType fromByte(Byte b) {
        int ordinal = b.intValue();
        if (ordinal < 0 || ordinal >= values().length) {
            return UNKNOWN;
        }
        return values()[ordinal];
    }
}
