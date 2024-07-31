/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.api;

import java.nio.ByteBuffer;
import java.util.Objects;

public class KeyValue {
    private final Key key;
    private final Value value;

    private KeyValue(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public static KeyValue of(String key, ByteBuffer value) {
        return new KeyValue(Key.of(key), Value.of(value));
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        KeyValue keyValue = (KeyValue) o;
        return Objects.equals(key, keyValue.key) && Objects.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KeyValue{" +
            "key=" + key +
            ", value=" + value +
            '}';
    }

    public static class Key {
        private final String key;

        private Key(String key) {
            this.key = key;
        }

        public static Key of(String key) {
            return new Key(key);
        }

        public String get() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key1 = (Key) o;
            return Objects.equals(key, key1.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public String toString() {
            return "Key{" +
                "key='" + key + '\'' +
                '}';
        }
    }

    public static class Value {
        private final ByteBuffer value;

        private Value(ByteBuffer value) {
            this.value = value;
        }

        public static Value of(ByteBuffer value) {
            return new Value(value);
        }

        public static Value of(byte[] value) {
            if (value == null) {
                return new Value(null);
            }
            return new Value(ByteBuffer.wrap(value));
        }

        public ByteBuffer get() {
            return value;
        }

        public boolean isNull() {
            return value == null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Value))
                return false;
            Value value1 = (Value) o;
            return Objects.equals(value, value1.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "Value{" +
                "value=" + value +
                '}';
        }
    }
}
