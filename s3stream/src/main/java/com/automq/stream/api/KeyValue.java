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

package com.automq.stream.api;

import java.nio.ByteBuffer;
import java.util.Objects;

public class KeyValue {
    private final Key key;
    private final Value value;
    private final String namespace;
    private final long epoch;

    private KeyValue(Key key, Value value) {
        this.key = key;
        this.value = value;
        this.namespace = null;
        this.epoch = 0L;
    }

    public KeyValue(Key key, Value value, String namespace, long epoch) {
        this.key = key;
        this.value = value;
        this.namespace = namespace;
        this.epoch = epoch;
    }

    public static KeyValue of(String key, ByteBuffer value) {
        return new KeyValue(Key.of(key), Value.of(value));
    }

    public static KeyValue of(String key, ByteBuffer value, String namespace, long epoch) {
        return new KeyValue(Key.of(key), Value.of(value), namespace, epoch);
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    public String namespace() {
        return namespace;
    }

    public long epoch() {
        return epoch;
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

    public static class KeyAndNamespace {
        private final Key key;
        private final String namespace;

        public KeyAndNamespace(Key key, String namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        public Key key() {
            return key;
        }

        public String namespace() {
            return namespace;
        }

        public static KeyAndNamespace of(String key, String namespace) {
            return new KeyAndNamespace(Key.of(key), namespace);
        }
    }

    public static class ValueAndEpoch {
        private final Value value;
        private final long epoch;

        public ValueAndEpoch(Value value, long epoch) {
            this.value = value;
            this.epoch = epoch;
        }

        public Value value() {
            return value;
        }

        public long epoch() {
            return epoch;
        }

        public static ValueAndEpoch of(byte[] value, long epoch) {
            return new ValueAndEpoch(Value.of(value), epoch);
        }

        public static ValueAndEpoch of(ByteBuffer value, long epoch) {
            return new ValueAndEpoch(Value.of(value), epoch);
        }
    }
}
