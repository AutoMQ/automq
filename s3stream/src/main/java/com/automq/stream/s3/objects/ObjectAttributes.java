/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
