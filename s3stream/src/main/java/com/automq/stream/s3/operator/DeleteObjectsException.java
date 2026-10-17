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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteObjectsException extends Exception {
    private static final int UNKNOWN_STATUS_CODE = -1;
    private final Set<String> successKeys;
    private final Map<String, DeleteObjectError> retriableKeys;
    private final Map<String, DeleteObjectError> failedKeys;

    public DeleteObjectsException(String message, List<String> failedKeys, List<String> errorsMessage) {
        super(message);
        this.successKeys = Set.of();
        this.retriableKeys = Map.of();
        Map<String, DeleteObjectError> keyErrors = new HashMap<>();
        for (int i = 0; i < failedKeys.size(); i++) {
            String errorMessage = i < errorsMessage.size() ? errorsMessage.get(i) : "";
            keyErrors.put(failedKeys.get(i), new DeleteObjectError("Unknown", UNKNOWN_STATUS_CODE, errorMessage));
        }
        this.failedKeys = Map.copyOf(keyErrors);
    }

    public DeleteObjectsException(String message, Set<String> successKeys,
        Map<String, DeleteObjectError> retriableKeys,
        Map<String, DeleteObjectError> failedKeys) {
        super(message);
        this.successKeys = Set.copyOf(successKeys);
        this.retriableKeys = Map.copyOf(retriableKeys);
        this.failedKeys = Map.copyOf(failedKeys);
    }

    public Set<String> getSuccessKeys() {
        return successKeys;
    }

    public Map<String, DeleteObjectError> getRetriableKeys() {
        return retriableKeys;
    }

    public List<String> getFailedKeys() {
        return List.copyOf(failedKeys.keySet());
    }

    public List<String> getErrorsMessages() {
        return failedKeys.values().stream().map(DeleteObjectError::message).toList();
    }

    public Map<String, DeleteObjectError> getFailedKeyErrors() {
        return failedKeys;
    }
}
