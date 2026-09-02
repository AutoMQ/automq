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

package kafka.log.streamaspect.reassignment;

import java.nio.ByteBuffer;

/**
 * One latest-value record captured from a frozen MetaStream.
 *
 * <p>The encoded bytes are the original MetaKeyValue payload stored at {@link #baseOffset()}.
 */
public final class MetaStreamHandoffRecord {
    private final long baseOffset;
    private final ByteBuffer encodedMetaKeyValue;

    /**
     * Creates an immutable copy of one frozen MetaStream record.
     *
     * @param baseOffset the record's original base offset
     * @param encodedMetaKeyValue the record's original encoded MetaKeyValue payload
     */
    public MetaStreamHandoffRecord(long baseOffset, ByteBuffer encodedMetaKeyValue) {
        this.baseOffset = baseOffset;
        ByteBuffer source = encodedMetaKeyValue.duplicate();
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source).flip();
        this.encodedMetaKeyValue = copy.asReadOnlyBuffer();
    }

    /**
     * Returns the record's original MetaStream base offset.
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * Returns an independent view of the original encoded MetaKeyValue bytes.
     */
    public ByteBuffer encodedMetaKeyValue() {
        return encodedMetaKeyValue.duplicate();
    }
}
