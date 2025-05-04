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

package com.automq.stream.s3;

public enum ByteBufAllocPolicy {

    /**
     * Allocate memory from the heap with pooling.
     */
    POOLED_HEAP(true, false),

    /**
     * Use pooled direct memory.
     */
    POOLED_DIRECT(true, true);

    /**
     * Whether the buffer should be pooled or not.
     */
    private final boolean pooled;

    /**
     * Whether the buffer should be direct or not.
     */
    private final boolean direct;

    ByteBufAllocPolicy(boolean pooled, boolean direct) {
        this.pooled = pooled;
        this.direct = direct;
    }

    public boolean isPooled() {
        return pooled;
    }

    public boolean isDirect() {
        return direct;
    }
}
