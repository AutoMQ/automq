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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;

import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy.SHUT_DOWN;

public class StrictBufferConfigImpl extends BufferConfigImpl<Suppressed.StrictBufferConfig> implements Suppressed.StrictBufferConfig {

    private final long maxKeys;
    private final long maxBytes;
    private final BufferFullStrategy bufferFullStrategy;

    public StrictBufferConfigImpl(final long maxKeys,
                                  final long maxBytes,
                                  final BufferFullStrategy bufferFullStrategy) {
        this.maxKeys = maxKeys;
        this.maxBytes = maxBytes;
        this.bufferFullStrategy = bufferFullStrategy;
    }

    public StrictBufferConfigImpl() {
        this.maxKeys = Long.MAX_VALUE;
        this.maxBytes = Long.MAX_VALUE;
        this.bufferFullStrategy = SHUT_DOWN;
    }

    @Override
    public Suppressed.StrictBufferConfig withMaxRecords(final long recordLimit) {
        return new StrictBufferConfigImpl(recordLimit, maxBytes, bufferFullStrategy);
    }

    @Override
    public Suppressed.StrictBufferConfig withMaxBytes(final long byteLimit) {
        return new StrictBufferConfigImpl(maxKeys, byteLimit, bufferFullStrategy);
    }

    @Override
    public long maxKeys() {
        return maxKeys;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
    public BufferFullStrategy bufferFullStrategy() {
        return bufferFullStrategy;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StrictBufferConfigImpl that = (StrictBufferConfigImpl) o;
        return maxKeys == that.maxKeys &&
            maxBytes == that.maxBytes &&
            bufferFullStrategy == that.bufferFullStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxKeys, maxBytes, bufferFullStrategy);
    }

    @Override
    public String toString() {
        return "StrictBufferConfigImpl{maxKeys=" + maxKeys +
            ", maxBytes=" + maxBytes +
            ", bufferFullStrategy=" + bufferFullStrategy + '}';
    }
}
