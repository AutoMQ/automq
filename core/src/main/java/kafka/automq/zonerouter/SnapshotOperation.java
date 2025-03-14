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

package kafka.automq.zonerouter;

public enum SnapshotOperation {
    ADD((short) 0), PATCH((short) 1), REMOVE((short) 2);

    final short code;

    SnapshotOperation(short code) {
        this.code = code;
    }

    public short code() {
        return code;
    }

    public static SnapshotOperation parse(short code) {
        switch (code) {
            case 0:
                return ADD;
            case 1:
                return PATCH;
            case 2:
                return REMOVE;
            default:
                throw new IllegalArgumentException("Unknown SnapshotOperation code: " + code);
        }
    }
}
