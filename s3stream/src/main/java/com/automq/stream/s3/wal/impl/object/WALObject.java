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

package com.automq.stream.s3.wal.impl.object;

import java.util.Objects;

public class WALObject implements Comparable<WALObject> {
    private final short bucketId;
    private final String path;
    private final long epoch;
    private final long startOffset;
    private final long endOffset;
    private final long length;

    public WALObject(short bucketId, String path, long epoch, long startOffset, long length) {
        this.bucketId = bucketId;
        this.path = path;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = WALObjectHeader.calculateEndOffsetV0(startOffset, length);
        this.length = length;
    }

    public WALObject(short bucketId, String path, long epoch, long startOffset, long endOffset, long length) {
        this.bucketId = bucketId;
        this.path = path;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.length = length;
    }

    @Override
    public int compareTo(WALObject o) {
        return Long.compare(startOffset, o.startOffset);
    }

    public short bucketId() {
        return bucketId;
    }

    public String path() {
        return path;
    }

    public long epoch() {
        return epoch;
    }

    public long startOffset() {
        return startOffset;
    }

    public long length() {
        return length;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "WALObject{" +
            "bucketId=" + bucketId +
            ", path='" + path + '\'' +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", length=" + length +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WALObject))
            return false;
        WALObject object = (WALObject) o;
        return bucketId == object.bucketId && startOffset == object.startOffset && endOffset == object.endOffset && length == object.length && Objects.equals(path, object.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, path, startOffset, endOffset, length);
    }
}
