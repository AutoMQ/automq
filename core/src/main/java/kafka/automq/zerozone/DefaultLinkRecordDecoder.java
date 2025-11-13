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

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;

import com.automq.stream.s3.cache.SnapshotReadCache;
import com.automq.stream.s3.model.StreamRecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultLinkRecordDecoder implements com.automq.stream.api.LinkRecordDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLinkRecordDecoder.class);
    private final RouterChannelProvider channelProvider;

    public DefaultLinkRecordDecoder(RouterChannelProvider channelProvider) {
        this.channelProvider = channelProvider;
    }

    @Override
    public int decodedSize(ByteBuf linkRecordBuf) {
        return LinkRecord.decodedSize(linkRecordBuf);
    }

    @Override
    public CompletableFuture<StreamRecordBatch> decode(StreamRecordBatch src) {
        try {
            LinkRecord linkRecord = LinkRecord.decode(src.getPayload());
            ChannelOffset channelOffset = linkRecord.channelOffset();
            RouterChannel routerChannel = channelProvider.readOnlyChannel(channelOffset.channelOwnerNodeId());
            return routerChannel.get(channelOffset.byteBuf()).thenApply(buf -> {
                try (ZoneRouterProduceRequest req = ZoneRouterPackReader.decodeDataBlock(buf).get(0)) {
                    MemoryRecords records = (MemoryRecords) (req.data().topicData().iterator().next()
                        .partitionData().iterator().next()
                        .records());
                    MutableRecordBatch recordBatch = records.batches().iterator().next();
                    recordBatch.setLastOffset(linkRecord.lastOffset());
                    recordBatch.setMaxTimestamp(linkRecord.timestampType(), linkRecord.maxTimestamp());
                    recordBatch.setPartitionLeaderEpoch(linkRecord.partitionLeaderEpoch());
                    StreamRecordBatch streamRecordBatch = new StreamRecordBatch(src.getStreamId(), src.getEpoch(), src.getBaseOffset(),
                        -src.getCount(), Unpooled.wrappedBuffer(records.buffer()));
                    // The buf will be release after the finally block, so we need copy the data by #encoded.
                    streamRecordBatch.encoded(SnapshotReadCache.ENCODE_ALLOC);
                    return streamRecordBatch;
                } finally {
                    buf.release();
                }
            }).whenComplete((rst, ex) -> {
                src.release();
                if (ex != null) {
                    LOGGER.error("Error while decoding link record, link={}", linkRecord, ex);
                }
            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
