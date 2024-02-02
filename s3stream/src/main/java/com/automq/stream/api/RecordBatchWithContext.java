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

package com.automq.stream.api;

public interface RecordBatchWithContext extends RecordBatch {

    /**
     * Get record batch base offset.
     *
     * @return base offset.
     */
    long baseOffset();

    /**
     * Get record batch exclusive last offset.
     *
     * @return exclusive last offset.
     */
    long lastOffset();
}
