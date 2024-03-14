/*
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

public enum S3Operation {
    /* S3 stream operations start */
    CREATE_STREAM(S3MetricsType.S3Stream, "create"),
    OPEN_STREAM(S3MetricsType.S3Stream, "open"),
    APPEND_STREAM(S3MetricsType.S3Stream, "append"),
    APPEND_STREAM_WRITE_LOCK(S3MetricsType.S3Stream, "append_write_lock"),
    FETCH_STREAM(S3MetricsType.S3Stream, "fetch"),
    FETCH_STREAM_READ_LOCK(S3MetricsType.S3Stream, "fetch_read_lock"),
    TRIM_STREAM(S3MetricsType.S3Stream, "trim"),
    CLOSE_STREAM(S3MetricsType.S3Stream, "close"),
    /* S3 stream operations end */

    /* S3 storage operations start */
    APPEND_STORAGE(S3MetricsType.S3Storage, "append"),
    APPEND_STORAGE_WAL(S3MetricsType.S3Storage, "append_wal"),
    APPEND_STORAGE_WAL_BEFORE(S3MetricsType.S3Storage, "append_wal_before"),
    APPEND_STORAGE_WAL_AWAIT(S3MetricsType.S3Storage, "append_wal_await"),
    APPEND_STORAGE_WAL_WRITE(S3MetricsType.S3Storage, "append_wal_write"),
    APPEND_STORAGE_WAL_AFTER(S3MetricsType.S3Storage, "append_wal_after"),
    APPEND_STORAGE_WAL_BLOCK_POLLED(S3MetricsType.S3Storage, "append_wal_block_polled"),
    APPEND_STORAGE_APPEND_CALLBACK(S3MetricsType.S3Storage, "append_callback"),
    APPEND_STORAGE_WAL_FULL(S3MetricsType.S3Storage, "append_wal_full"),
    APPEND_STORAGE_LOG_CACHE(S3MetricsType.S3Storage, "append_log_cache"),
    APPEND_STORAGE_LOG_CACHE_FULL(S3MetricsType.S3Storage, "append_log_cache_full"),
    UPLOAD_STORAGE_WAL(S3MetricsType.S3Storage, "upload_wal"),
    READ_STORAGE(S3MetricsType.S3Storage, "read"),
    READ_STORAGE_LOG_CACHE(S3MetricsType.S3Storage, "read_log_cache"),
    READ_STORAGE_LOG_CACHE_MISS(S3MetricsType.S3Storage, "read_log_cache_miss"),
    READ_STORAGE_BLOCK_CACHE(S3MetricsType.S3Storage, "read_block_cache"),
    READ_STORAGE_BLOCK_CACHE_MISS(S3MetricsType.S3Storage, "read_block_cache_miss"),
    /* S3 storage operations end */

    /* S3 request operations start */
    GET_OBJECT(S3MetricsType.S3Request, "get_object"),
    GET_OBJECT_FAIL(S3MetricsType.S3Request, "get_object_fail"),
    PUT_OBJECT(S3MetricsType.S3Request, "put_object"),
    PUT_OBJECT_FAIL(S3MetricsType.S3Request, "put_object_fail"),
    DELETE_OBJECT(S3MetricsType.S3Request, "delete_object"),
    DELETE_OBJECT_FAIL(S3MetricsType.S3Request, "delete_object_fail"),
    DELETE_OBJECTS(S3MetricsType.S3Request, "delete_objects"),
    DELETE_OBJECTS_FAIL(S3MetricsType.S3Request, "delete_objects_fail"),
    CREATE_MULTI_PART_UPLOAD(S3MetricsType.S3Request, "create_multi_part_upload"),
    CREATE_MULTI_PART_UPLOAD_FAIL(S3MetricsType.S3Request, "create_multi_part_upload_fail"),
    UPLOAD_PART(S3MetricsType.S3Request, "upload_part"),
    UPLOAD_PART_FAIL(S3MetricsType.S3Request, "upload_part_fail"),
    UPLOAD_PART_COPY(S3MetricsType.S3Request, "upload_part_copy"),
    UPLOAD_PART_COPY_FAIL(S3MetricsType.S3Request, "upload_part_copy_fail"),
    COMPLETE_MULTI_PART_UPLOAD(S3MetricsType.S3Request, "complete_multi_part_upload"),
    COMPLETE_MULTI_PART_UPLOAD_FAIL(S3MetricsType.S3Request, "complete_multi_part_upload_fail"),
    /* S3 request operations end */

    /* S3 object operations start */
    PREPARE_OBJECT(S3MetricsType.S3Object, "prepare"),
    COMMIT_STREAM_SET_OBJECT(S3MetricsType.S3Object, "commit_stream_set_object"),
    COMPACTED_OBJECT(S3MetricsType.S3Object, "compacted_object"),
    COMMIT_STREAM_OBJECT(S3MetricsType.S3Object, "commit_stream_object"),
    GET_OBJECTS(S3MetricsType.S3Object, "get_objects"),
    GET_SERVER_OBJECTS(S3MetricsType.S3Object, "get_server_objects"),
    GET_STREAM_OBJECTS(S3MetricsType.S3Object, "get_stream_objects"),
    /* S3 object operations end */

    ALLOC_BUFFER(S3MetricsType.S3Storage, "alloc_buffer");

    private final S3MetricsType type;
    private final String name;
    private final String uniqueKey;

    S3Operation(S3MetricsType type, String name) {
        this.type = type;
        this.name = name;
        uniqueKey = type + "-" + name;
    }

    public String getName() {
        return name;
    }

    public S3MetricsType getType() {
        return type;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    @Override
    public String toString() {
        return "Operation{" +
                "type='" + type.getName() + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
