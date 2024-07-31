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

package com.automq.stream.s3.wal.common;

import com.automq.stream.s3.wal.RecoverResult;
import io.netty.buffer.ByteBuf;
import java.util.Objects;

public class RecoverResultImpl implements RecoverResult {
    private final ByteBuf record;
    private final long recordOffset;

    public RecoverResultImpl(ByteBuf record, long recordOffset) {
        this.record = record;
        this.recordOffset = recordOffset;
    }

    @Override
    public String toString() {
        return "RecoverResultImpl{"
               + "record=" + record
               + ", recordOffset=" + recordOffset
               + '}';
    }

    @Override
    public ByteBuf record() {
        return record;
    }

    @Override
    public long recordOffset() {
        return recordOffset;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (RecoverResultImpl) obj;
        return Objects.equals(this.record, that.record) &&
               this.recordOffset == that.recordOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, recordOffset);
    }

}
