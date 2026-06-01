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

package com.automq.stream.s3.operator;

public class DeleteObjectError {
    private final String code;
    private final int statusCode;
    private final String message;

    public DeleteObjectError(String code, int statusCode, String message) {
        this.code = code == null ? "Unknown" : code;
        this.statusCode = statusCode;
        this.message = message == null ? "" : message;
    }

    public String code() {
        return code;
    }

    public int statusCode() {
        return statusCode;
    }

    public String message() {
        return message;
    }

    @Override
    public String toString() {
        return "code=" + code + ",status=" + statusCode + ",message=" + message;
    }
}
