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

package kafka.log.s3;

import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.Stream;
import com.automq.elasticstream.client.api.StreamClient;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.streams.StreamManager;

import java.util.concurrent.CompletableFuture;

public class S3StreamClient implements StreamClient {

    private final StreamManager streamController;
    private final Wal wal;
    private final S3BlockCache blockCache;
    private final ObjectManager objectManager;

    public S3StreamClient(StreamManager streamController, Wal wal, S3BlockCache blockCache, ObjectManager objectManager) {
        this.streamController = streamController;
        this.wal = wal;
        this.blockCache = blockCache;
        this.objectManager = objectManager;
    }

    @Override
    public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
        return streamController.createStream().thenCompose(streamId -> openStream0(streamId, options.epoch()));
    }

    @Override
    public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
        return openStream0(streamId, openStreamOptions.epoch());
    }

    private CompletableFuture<Stream> openStream0(long streamId, long epoch) {
        return streamController.openStream(streamId, epoch).
            thenApply(metadata -> new S3Stream(
                metadata.getStreamId(), metadata.getEpoch(),
                metadata.getStartOffset(), metadata.getNextOffset(),
                wal, blockCache, streamController));
    }
}
