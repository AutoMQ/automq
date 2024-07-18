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

package com.automq.stream.s3.wal.common;

import java.util.Objects;

public enum ShutdownType {
    /**
     * shutdown gracefully
     */
    GRACEFULLY(0),

    /**
     * shutdown ungracefully
     */
    UNGRACEFULLY(1);

    private final Integer code;

    ShutdownType(Integer code) {
        this.code = code;
    }

    public static ShutdownType fromCode(Integer code) {
        for (ShutdownType type : ShutdownType.values()) {
            if (Objects.equals(type.getCode(), code)) {
                return type;
            }
        }
        return null;
    }

    public Integer getCode() {
        return code;
    }
}
