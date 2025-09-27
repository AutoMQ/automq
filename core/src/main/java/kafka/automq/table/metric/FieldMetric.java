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

package kafka.automq.table.metric;

import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;

public final class FieldMetric {

    private static final int STRING_BASE_COST = 3;              // base cost for small strings
    private static final int STRING_UNIT_BYTES = 32;           // granularity for string scaling
    private static final int STRING_UNIT_STEP = 1;              // aggressive scaling for long strings

    private static final int BINARY_BASE_COST = 4;              // small binary payloads slightly heavier than primitives
    private static final int BINARY_UNIT_BYTES = 32;           // granularity for binary buffers
    private static final int BINARY_UNIT_STEP = 1;              // scaling factor for binary payloads

    private FieldMetric() {
    }

    public static int count(CharSequence value) {
        if (value == null) {
            return 0;
        }
        int lengthBytes = value instanceof Utf8
            ? ((Utf8) value).getByteLength()
            : value.length();

        if (lengthBytes <= STRING_UNIT_BYTES) {
            return STRING_BASE_COST;
        }
        int segments = (lengthBytes + STRING_UNIT_BYTES - 1) / STRING_UNIT_BYTES;
        return STRING_BASE_COST + (segments - 1) * STRING_UNIT_STEP;
    }

    public static int count(ByteBuffer value) {
        if (value == null) {
            return 0;
        }
        int remaining = value.remaining();
        if (remaining <= BINARY_UNIT_BYTES) {
            return BINARY_BASE_COST;
        }
        int segments = (remaining + BINARY_UNIT_BYTES - 1) / BINARY_UNIT_BYTES;
        return BINARY_BASE_COST + (segments - 1) * BINARY_UNIT_STEP;
    }

    public static int count(byte[] value) {
        if (value == null) {
            return 0;
        }
        int length = value.length;
        if (length <= BINARY_UNIT_BYTES) {
            return BINARY_BASE_COST;
        }
        int segments = (length + BINARY_UNIT_BYTES - 1) / BINARY_UNIT_BYTES;
        return BINARY_BASE_COST + (segments - 1) * BINARY_UNIT_STEP;
    }
}
