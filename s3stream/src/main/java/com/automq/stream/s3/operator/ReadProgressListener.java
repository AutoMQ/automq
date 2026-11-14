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

import java.util.List;

import io.netty.buffer.ByteBuf;

/**
 * Observes cumulative response chunks starting at the physical GET's requested start.
 * Each attempt reuses its own list and may append only after the synchronous callback returns.
 * Published chunks keep their content and readable range unchanged; they must not depend on a
 * growing composite buffer. The list and buffers are borrowed during the callback and must not
 * be modified or released. Consumers must retain required slices before returning and must not
 * pass the borrowed list to another thread.
 * Notifications may be concurrent across retries and their lengths need not increase. Already
 * delivered ranges remain successful if the rest of a response fails. Implementations may defer
 * notifications until completion when integrity requires the complete body. Listener exceptions
 * are logged without failing the download or releasing its response.
 */
@FunctionalInterface
public interface ReadProgressListener {
    /** Reports ordered chunks and their total readable bytes without running caller callbacks. */
    void onChange(List<ByteBuf> chunks, int readableBytes);
}
