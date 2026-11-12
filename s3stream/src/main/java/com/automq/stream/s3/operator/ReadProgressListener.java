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

package com.automq.stream.s3.operator;

import io.netty.buffer.ByteBuf;

/**
 * Observes one physical download attempt. Notifications contain the cumulative prefix of the requested
 * range. The read-only buffer is borrowed only for the callback; it must not be retained or modified.
 * Consumers must copy bytes they keep and independently validate any data exposed before completion.
 * Retries and speculative attempts have separate listeners and may run concurrently. Callbacks for
 * one attempt are serialized. Implementations may omit notifications, especially when response integrity
 * can only be checked at completion.
 */
public interface ReadProgressListener {
    /** Reports the contiguous bytes received so far; must not block or run arbitrary caller work on the transport thread. */
    void onProgress(ByteBuf prefix);

    /** Discards this attempt's partial state after a transport or callback failure. */
    void onFailure();
}
