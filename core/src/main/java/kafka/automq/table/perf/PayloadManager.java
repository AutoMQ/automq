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

package kafka.automq.table.perf;

import java.util.function.Supplier;

public class PayloadManager {
    private final byte[][] payloadPool;
    private final int poolSize;
    private int currentIndex = 0;

    public PayloadManager(Supplier<byte[]> payloadGenerator, int poolSize) {
        this.poolSize = Math.min(poolSize, 10000); // Limit max pool size
        this.payloadPool = new byte[this.poolSize][];

        // Pre-generate all payloads
        for (int i = 0; i < this.poolSize; i++) {
            this.payloadPool[i] = payloadGenerator.get();
        }
    }

    public byte[] nextPayload() {
        byte[] payload = payloadPool[currentIndex];
        currentIndex = (currentIndex + 1) % poolSize;
        return payload;
    }

    public void reset() {
        currentIndex = 0;
    }

    public int getPoolSize() {
        return poolSize;
    }
}
