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
import kafka.log.s3.streams.StreamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static kafka.log.es.FutureUtil.exec;

public class S3StreamClient implements StreamClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamClient.class);

    private final StreamManager streamManager;
    private final Storage storage;

    public S3StreamClient(StreamManager streamManager, Storage storage) {
        this.streamManager = streamManager;
        this.storage = storage;
    }

    @Override
    public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
        return exec(() -> streamManager.createStream().thenCompose(streamId -> openStream0(streamId, options.epoch())),
                LOGGER, "createAndOpenStream");
    }

    @Override
    public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
        return exec(() -> openStream0(streamId, openStreamOptions.epoch()), LOGGER, "openStream");
    }

    private CompletableFuture<Stream> openStream0(long streamId, long epoch) {
        return streamManager.openStream(streamId, epoch).
                thenApply(metadata -> new S3Stream(
                        metadata.getStreamId(), metadata.getEpoch(),
                        metadata.getStartOffset(), metadata.getNextOffset(),
                        storage, streamManager));
    }
}
