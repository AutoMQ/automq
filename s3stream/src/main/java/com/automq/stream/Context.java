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

package com.automq.stream;

import com.automq.stream.api.KVClient;
import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.s3.cache.SnapshotReadCache;

public class Context {
    private SnapshotReadCache snapshotReadCache;
    private ConfirmWAL confirmWAL;
    private KVClient kvClient;

    public static final Context INSTANCE = new Context();

    public static Context instance() {
        return INSTANCE;
    }

    public KVClient kvClient() {
        return kvClient;
    }

    public void kvClient(KVClient kvClient) {
        this.kvClient = kvClient;
    }

    public void snapshotReadCache(SnapshotReadCache snapshotReadCache) {
        this.snapshotReadCache = snapshotReadCache;
    }

    public SnapshotReadCache snapshotReadCache() {
        return snapshotReadCache;
    }

    public void confirmWAL(ConfirmWAL confirmWAL) {
        this.confirmWAL = confirmWAL;
    }

    public ConfirmWAL confirmWAL() {
        return confirmWAL;
    }

}
