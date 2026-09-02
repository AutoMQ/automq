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
package com.automq.stream.utils;

import java.util.concurrent.locks.Lock;

/** Executes actions while holding an explicit lock and always releases the lock afterward. */
public final class LockUtils {
    private LockUtils() {
    }

    /**
     * Executes an action while holding the supplied lock.
     *
     * @param lock lock guarding the action
     * @param action action to execute
     * @param <E> checked or unchecked failure type
     * @throws E when the action fails
     */
    public static <E extends Throwable> void runInLock(Lock lock, ThrowingRunnable<E> action) throws E {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Computes a value while holding the supplied lock.
     *
     * @param lock lock guarding the action
     * @param action value-producing action to execute
     * @param <T> returned value type
     * @param <E> checked or unchecked failure type
     * @return computed value
     * @throws E when the action fails
     */
    public static <T, E extends Throwable> T runInLock(Lock lock, ThrowingSupplier<T, E> action) throws E {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    /** An action that may fail with a checked exception. */
    @FunctionalInterface
    public interface ThrowingRunnable<E extends Throwable> {
        void run() throws E;
    }

    /** A value-producing action that may fail with a checked exception. */
    @FunctionalInterface
    public interface ThrowingSupplier<T, E extends Throwable> {
        T get() throws E;
    }
}
