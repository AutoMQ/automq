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

package com.automq.stream.s3.metrics.operations;

public enum S3Stage {

    /* Append WAL stages start */
    APPEND_WAL_BEFORE(S3Operation.APPEND_STORAGE_WAL, "before"),
    APPEND_WAL_BLOCK_POLLED(S3Operation.APPEND_STORAGE_WAL, "block_polled"),
    APPEND_WAL_AWAIT(S3Operation.APPEND_STORAGE_WAL, "await"),
    APPEND_WAL_WRITE(S3Operation.APPEND_STORAGE_WAL, "write"),
    APPEND_WAL_AFTER(S3Operation.APPEND_STORAGE_WAL, "after"),
    APPEND_WAL_COMPLETE(S3Operation.APPEND_STORAGE_WAL, "complete"),
    /* Append WAL stages end */

    /* Force upload WAL start */
    FORCE_UPLOAD_WAL_AWAIT(S3Operation.FORCE_UPLOAD_STORAGE_WAL, "await"),
    FORCE_UPLOAD_WAL_COMPLETE(S3Operation.FORCE_UPLOAD_STORAGE_WAL, "complete"),
    /* Force upload WAL end */

    /* Upload WAL start */
    UPLOAD_WAL_PREPARE(S3Operation.UPLOAD_STORAGE_WAL, "prepare"),
    UPLOAD_WAL_UPLOAD(S3Operation.UPLOAD_STORAGE_WAL, "upload"),
    UPLOAD_WAL_COMMIT(S3Operation.UPLOAD_STORAGE_WAL, "commit"),
    UPLOAD_WAL_COMPLETE(S3Operation.UPLOAD_STORAGE_WAL, "complete");
    /* Upload WAL end */

    private final S3Operation operation;
    private final String name;

    S3Stage(S3Operation operation, String name) {
        this.operation = operation;
        this.name = name;
    }

    public S3Operation getOperation() {
        return operation;
    }

    public String getName() {
        return name;
    }

    public String getUniqueKey() {
        return operation.getUniqueKey() + "-" + name;
    }

    @Override
    public String toString() {
        return "S3Stage{" +
            "operation=" + operation.getName() +
            ", name='" + name + '\'' +
            '}';
    }
}
