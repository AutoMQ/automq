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

package com.automq.stream.s3.objects;

/**
 * Object attributes, bits:
 * 0~1: object type
 * 0: normal object
 * 1: composite object
 * 2~17: bucket index
 * 18: deep delete mark
 * 19~31 unused
 */
public class ObjectAttributes {
    public static final ObjectAttributes DEFAULT = new ObjectAttributes(0);
    public static final ObjectAttributes UNSET = new ObjectAttributes(-1);
    public static final short MATCH_ALL_BUCKET = (short) -1;
    private static final int DEEP_DELETE_MASK = 1 << 18;
    private final int attributes;

    private ObjectAttributes(int attributes) {
        this.attributes = attributes;
    }

    public Type type() {
        return Type.from(attributes & 0x3);
    }

    public short bucket() {
        return (short) ((attributes >> 2) & 0xFFFF);
    }

    public boolean deepDelete() {
        return (attributes & DEEP_DELETE_MASK) != 0;
    }

    public int attributes() {
        return attributes;
    }

    public static ObjectAttributes from(int attributes) {
        return new ObjectAttributes(attributes);
    }

    public static Builder builder() {
        return builder(0);
    }

    public static Builder builder(int attributes) {
        return new Builder(attributes);
    }

    public static class Builder {
        private int attributes;

        public Builder(int attributes) {
            this.attributes = attributes;
        }

        public Builder type(Type type) {
            switch (type) {
                case Normal:
                    break;
                case Composite:
                    attributes |= 1;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + type);
            }
            return this;
        }

        public Builder bucket(short bucketIndex) {
            attributes |= bucketIndex << 2;
            return this;
        }

        public Builder deepDelete() {
            attributes |= DEEP_DELETE_MASK;
            return this;
        }

        public ObjectAttributes build() {
            return new ObjectAttributes(attributes);
        }
    }

    public enum Type {
        Normal((byte) 0),
        Composite((byte) 1);

        private final byte value;

        Type(byte value) {
            this.value = value;
        }

        public static Type from(int value) {
            switch (value) {
                case 0:
                    return Normal;
                case 1:
                    return Composite;
                default:
                    throw new IllegalArgumentException("Unknown type: " + value);
            }
        }

        public byte value() {
            return value;
        }
    }

}
