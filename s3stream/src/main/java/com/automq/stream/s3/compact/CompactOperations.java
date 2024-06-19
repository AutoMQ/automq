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

package com.automq.stream.s3.compact;

public enum CompactOperations {
    DELETE((byte) 0),
    KEEP_DATA((byte) 1);

    private final byte value;

    CompactOperations(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static CompactOperations fromValue(byte value) {
        switch (value) {
            case 0:
                return DELETE;
            case 1:
                return KEEP_DATA;
            default:
                throw new IllegalArgumentException("Unknown value: " + value);
        }
    }

}
