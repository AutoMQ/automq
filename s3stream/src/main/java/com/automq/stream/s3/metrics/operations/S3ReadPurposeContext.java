/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.metrics.operations;

import java.util.Objects;
import java.util.function.Supplier;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Carries metric-only read purpose through synchronous calls. Asynchronous boundaries must
 * capture the current value into their task or explicitly re-establish it in their callback.
 */
public final class S3ReadPurposeContext {
    private static final FastThreadLocal<S3ReadPurpose> CURRENT = new FastThreadLocal<>();

    private S3ReadPurposeContext() {
    }

    /** Returns this thread's scoped purpose, or OTHER for an unannotated caller. */
    public static S3ReadPurpose current() {
        S3ReadPurpose purpose = CURRENT.get();
        return purpose == null ? S3ReadPurpose.OTHER : purpose;
    }

    /**
     * Runs the action with a purpose and restores the previous value on return or failure.
     * If the action returns a future, the scope ends immediately; it does not follow completion.
     */
    public static <T> T withPurpose(S3ReadPurpose purpose, Supplier<T> action) {
        Objects.requireNonNull(purpose, "purpose");
        S3ReadPurpose previous = CURRENT.get();
        CURRENT.set(purpose);
        try {
            return action.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }
}
