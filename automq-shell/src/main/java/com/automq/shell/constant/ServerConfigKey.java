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
package com.automq.shell.constant;

public enum ServerConfigKey {

    NODE_ID("node.id"),
    CONTROLLER_QUORUM_VOTERS("controller.quorum.voters"),

    LISTENERS("listeners"),

    ADVERTISED_LISTENERS("advertised.listeners"),

    S3_ENDPOINT("s3.endpoint"),

    S3_REGION("s3.region"),

    S3_BUCKET("s3.bucket"),

    S3_PATH_STYLE("s3.path.style");

    ServerConfigKey(String keyName) {
        this.keyName = keyName;
    }

    private final String keyName;

    public String getKeyName() {
        return keyName;
    }
}
