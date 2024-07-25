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

package com.automq.stream.s3.compact;

public enum CompactOperations {
    // - normal object: delete the object
    // - composite object: delete the composite object
    DELETE((byte) 0),
    // only delete the metadata in KRaft
    KEEP_DATA((byte) 1),
    // - normal object: delete the object
    // - composite object: delete the composite object and all its linked objects
    DEEP_DELETE((byte) 2);

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
            case 2:
                return DEEP_DELETE;
            default:
                throw new IllegalArgumentException("Unknown value: " + value);
        }
    }

}
