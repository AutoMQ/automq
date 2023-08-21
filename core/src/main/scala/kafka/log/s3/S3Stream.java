/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.s3;

import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.Stream;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.model.StreamMetadata;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.streams.StreamManager;

import java.util.concurrent.CompletableFuture;

public class S3Stream implements Stream {
    private final StreamMetadata metadata;
    private final Wal wal;
    private final S3BlockCache blockCache;
    private final StreamManager streamManager;
    private final ObjectManager objectManager;

    public S3Stream(StreamMetadata metadata, Wal wal, S3BlockCache blockCache, StreamManager streamManager, ObjectManager objectManager) {
        this.metadata = metadata;
        this.wal = wal;
        this.blockCache = blockCache;
        this.streamManager = streamManager;
        this.objectManager = objectManager;
    }

    @Override
    public long streamId() {
        return metadata.getStreamId();
    }

    @Override
    public long startOffset() {
        return metadata.getStartOffset();
    }

    @Override
    public long nextOffset() {
        return 0;
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        return null;
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytes) {
        return null;
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        return streamManager.trimStream(metadata.getStreamId(), metadata.getEpoch(), newStartOffset);
    }

    @Override
    public CompletableFuture<Void> close() {
        return null;
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return null;
    }
}
