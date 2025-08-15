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

package kafka.automq.zerozone;

import com.automq.stream.Context;
import com.automq.stream.s3.cache.SnapshotReadCache;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DefaultReplayer implements Replayer {
    private SnapshotReadCache.EventListener listener;
    private SnapshotReadCache snapshotReadCache;

    @Override
    public CompletableFuture<Void> replay(List<S3ObjectMetadata> objects) {
        return snapshotReadCache().replay(objects);
    }

    @Override
    public CompletableFuture<Void> replay(WriteAheadLog confirmWAL, RecordOffset startOffset, RecordOffset endOffset) {
        return snapshotReadCache().replay(confirmWAL, startOffset, endOffset);
    }

    private SnapshotReadCache snapshotReadCache() {
        if (snapshotReadCache == null) {
            snapshotReadCache = Context.instance().snapshotReadCache();
            if (listener != null) {
                snapshotReadCache.addEventListener(listener);
            }
        }
        return snapshotReadCache;
    }

    public void setCacheEventListener(SnapshotReadCache.EventListener listener) {
        this.listener = listener;
        if (snapshotReadCache != null) {
            snapshotReadCache.addEventListener(listener);
        }
    }
}
