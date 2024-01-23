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
package com.automq.s3shell.sdk.model;

public enum AuthMethod {
    KEY_FROM_ENV("key-from-env"),

    KEY_FROM_ARGS("key-from-args"),

    // assume role
    USER_ROLE("user-role"),

    // instance profile
    INSTANCE_ROLE("instance-role");

    AuthMethod(String keyName) {
        this.keyName = keyName;
    }

    private final String keyName;

    public String getKeyName() {
        return keyName;
    }

    public static AuthMethod getByName(String methodName) {
        for (AuthMethod method : AuthMethod.values()) {
            if (method.getKeyName().equals(methodName)) {
                return method;
            }
        }
        throw new IllegalArgumentException("Invalid auth method: " + methodName);
    }
}