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

package kafka.automq.partition.snapshot;

public enum SnapshotOperation {
    ADD((short) 0), PATCH((short) 1), REMOVE((short) 2);

    final short code;

    SnapshotOperation(short code) {
        this.code = code;
    }

    public short code() {
        return code;
    }

    public static SnapshotOperation parse(short code) {
        switch (code) {
            case 0:
                return ADD;
            case 1:
                return PATCH;
            case 2:
                return REMOVE;
            default:
                throw new IllegalArgumentException("Unknown SnapshotOperation code: " + code);
        }
    }
}
