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

package kafka.automq.table.perf;

import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public enum DataType {
    BOOLEAN("boolean", () -> ThreadLocalRandom.current().nextBoolean()),
    INT("int", () -> ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)),
    LONG("long", () -> ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
    DOUBLE("double", () -> ThreadLocalRandom.current().nextDouble(Long.MAX_VALUE)),
    TIMESTAMP("timestamp", () -> ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
    STRING("string", () -> RandomStringUtils.randomAlphabetic(32)),
    BINARY("binary", () -> {
        byte[] bytes = new byte[32];
        ThreadLocalRandom.current().nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }),
    NESTED("nested", null),
    ARRAY("array", null);

    private final String name;
    // Supplier holds runtime-only generators (often non-serializable lambdas). Enum
    // instances are serialized by name, not by fields; mark this field transient to
    // avoid accidental Java serialization of the supplier and silence static analyzers.
    private final transient Supplier<Object> valueGenerator;

    DataType(String name, Supplier<Object> valueGenerator) {
        this.name = name;
        this.valueGenerator = valueGenerator;
    }

    public String getName() {
        return name;
    }

    public Object generateValue() {
        if (valueGenerator == null) {
            throw new UnsupportedOperationException("Complex type " + name + " requires specific generator");
        }
        return valueGenerator.get();
    }

    public static DataType fromString(String name) {
        for (DataType type : values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }
        return null;
    }

    public PerfTestCase createAvroTestCase(int fieldCount, int payloadCount, PerfConfig config) {
        return new AvroTestCase(this, fieldCount, payloadCount, config);
    }

    public PerfTestCase createProtobufTestCase(int fieldCount, int payloadCount, PerfConfig config) {
        return new ProtobufTestCase(this, fieldCount, payloadCount, config);
    }
}
