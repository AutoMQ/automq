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

package org.apache.kafka.metadata.stream;

public enum S3ObjectState {
    UNINITIALIZED((byte) 0),
    PREPARED((byte) 1),
    COMMITTED((byte) 2),
    MARK_DESTROYED((byte) 3),
    DESTROYED((byte) 4);

    private final byte value;

    S3ObjectState(byte value) {
        this.value = value;
    }

    public byte toByte() {
        return value;
    }

    public static S3ObjectState fromByte(Byte b) {
        switch (b) {
            case 0:
                return UNINITIALIZED;
            case 1:
                return PREPARED;
            case 2:
                return COMMITTED;
            case 3:
                return MARK_DESTROYED;
            case 4:
                return DESTROYED;
            default:
                throw new IllegalArgumentException("Unknown value: " + b);
        }
    }
}