/*
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
package org.apache.kafka.coordinator.group.runtime;

import java.util.concurrent.TimeUnit;

/**
 * An interface to schedule and cancel operations.
 */
public interface Timer {

    /**
     * Add an operation to the timer. If an operation with the same key
     * already exists, replace it with the new operation.
     *
     * @param key         The key to identify this operation.
     * @param delay       The delay to wait before expiring.
     * @param unit        The delay unit.
     * @param operation   The operation to perform upon expiration.
     */
    void schedule(String key, long delay, TimeUnit unit, Runnable operation);

    /**
     * Remove an operation corresponding to a given key.
     *
     * @param key The key.
     */
    void cancel(String key);
}
