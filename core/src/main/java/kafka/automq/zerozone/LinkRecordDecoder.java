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

import com.automq.stream.s3.model.StreamRecordBatch;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.netty.buffer.Unpooled;

public class LinkRecordDecoder implements Function<StreamRecordBatch, CompletableFuture<StreamRecordBatch>> {
    private final RouterChannelProvider channelProvider;

    public LinkRecordDecoder(RouterChannelProvider channelProvider) {
        this.channelProvider = channelProvider;
    }

    @Override
    public CompletableFuture<StreamRecordBatch> apply(StreamRecordBatch src) {
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
                    // duplicated copy the payload
                    streamRecordBatch.encoded();
                    return streamRecordBatch;
                } finally {
                    src.release();
                    buf.release();
                }
            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
