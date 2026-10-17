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

import java.util.List;

/**
 * Immutable latest-KV payload and end offset captured at one frozen MetaStream consistency point.
 */
public final class MetaStreamHandoff {
    private final long endOffset;
    private final List<MetaStreamHandoffRecord> records;

    /**
     * Creates a handoff for one frozen consistency point.
     *
     * @param endOffset the frozen exclusive end offset
     * @param records the latest record for every metadata key
     */
    public MetaStreamHandoff(long endOffset, List<MetaStreamHandoffRecord> records) {
        this.endOffset = endOffset;
        this.records = List.copyOf(records);
    }

    /**
     * Returns the exclusive MetaStream end offset at the frozen consistency point.
     */
    public long endOffset() {
        return endOffset;
    }

    /**
     * Returns the latest record for every key, ordered by original base offset.
     */
    public List<MetaStreamHandoffRecord> records() {
        return records;
    }
}
