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


package kafka.log.streamaspect;

public enum ElasticResourceStatus {
    FENCED(false),
    CLOSED(false),
    OK(true);

    private final boolean writable;

    ElasticResourceStatus(boolean writable) {
        this.writable = writable;
    }

    public boolean writable() {
        return writable;
    }
}
