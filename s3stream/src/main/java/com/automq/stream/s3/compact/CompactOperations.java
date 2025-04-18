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

package com.automq.stream.s3.compact;

public enum CompactOperations {
    // - normal object: delete the object
    // - composite object: delete the composite object
    DELETE((byte) 0),
    // only delete the metadata in KRaft
    KEEP_DATA((byte) 1),
    // - normal object: delete the object
    // - composite object: delete the composite object and all its linked objects
    DEEP_DELETE((byte) 2);

    private final byte value;

    CompactOperations(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static CompactOperations fromValue(byte value) {
        switch (value) {
            case 0:
                return DELETE;
            case 1:
                return KEEP_DATA;
            case 2:
                return DEEP_DELETE;
            default:
                throw new IllegalArgumentException("Unknown value: " + value);
        }
    }

}
