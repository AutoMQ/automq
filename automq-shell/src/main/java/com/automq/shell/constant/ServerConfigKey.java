/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
