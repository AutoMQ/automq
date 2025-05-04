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

package kafka.log.streamaspect;

import org.apache.kafka.common.utils.Utils;

import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.utils.FutureUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DefaultElasticStreamSlice implements ElasticStreamSlice {
    /**
     * the real start offset of this segment in the stream.
     */
    private final long startOffsetInStream;
    private final Stream stream;
    // The relative endOffset of sealed stream slice
    private long endOffset = Offsets.NOOP_OFFSET;
    private boolean sealed = false;

    public DefaultElasticStreamSlice(Stream stream, SliceRange sliceRange) {
        this.stream = stream;
        long streamNextOffset = stream.nextOffset();
        if (sliceRange.start() == Offsets.NOOP_OFFSET) {
            // new stream slice
            this.startOffsetInStream = streamNextOffset;
            sliceRange.start(startOffsetInStream);
        } else if (sliceRange.end() == Offsets.NOOP_OFFSET) {
            // unsealed stream slice
            this.startOffsetInStream = sliceRange.start();
        } else {
            // sealed stream slice
            this.startOffsetInStream = sliceRange.start();
            this.endOffset = sliceRange.end() - startOffsetInStream;
            this.sealed = true;
        }
    }

    @Override
    public CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
        if (sealed) {
            return FutureUtil.failedFuture(new IllegalStateException("stream segment " + this + " is sealed"));
        }
        return stream.append(context, recordBatch).thenApply(AppendResultWrapper::new);
    }

    @Override
    public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint) {
        long fixedStartOffset = Utils.max(startOffset, 0);
        return stream.fetch(context, startOffsetInStream + fixedStartOffset, startOffsetInStream + endOffset, maxBytesHint)
                .thenApply(FetchResultWrapper::new);
    }

    @Override
    public long nextOffset() {
        return endOffset != Offsets.NOOP_OFFSET ? endOffset : (stream.nextOffset() - startOffsetInStream);
    }

    @Override
    public long confirmOffset() {
        return endOffset != Offsets.NOOP_OFFSET ? endOffset : (stream.confirmOffset() - startOffsetInStream);
    }

    @Override
    public SliceRange sliceRange() {
        if (sealed) {
            return SliceRange.of(startOffsetInStream, startOffsetInStream + endOffset);
        } else {
            return SliceRange.of(startOffsetInStream, Offsets.NOOP_OFFSET);
        }
    }

    @Override
    public void seal() {
        if (!sealed) {
            sealed = true;
            endOffset = stream.nextOffset() - startOffsetInStream;
        }
    }

    @Override
    public Stream stream() {
        return stream;
    }

    @Override
    public String toString() {
        return "DefaultElasticStreamSlice{" +
                ", streamId=" + stream.streamId() +
                ", slice=" + sliceRange() +
                ", nextOffset=" + nextOffset() +
                ", sealed=" + sealed +
                '}';
    }

    class AppendResultWrapper implements AppendResult {
        private final AppendResult inner;

        public AppendResultWrapper(AppendResult inner) {
            this.inner = inner;
        }

        @Override
        public long baseOffset() {
            return inner.baseOffset() - startOffsetInStream;
        }
    }

    class FetchResultWrapper implements FetchResult {
        private final FetchResult inner;
        private final List<RecordBatchWithContext> recordBatchList;

        public FetchResultWrapper(FetchResult fetchResult) {
            this.inner = fetchResult;
            this.recordBatchList = new ArrayList<>(fetchResult.recordBatchList().size());
            for (RecordBatchWithContext recordBatchWithContext : fetchResult.recordBatchList()) {
                this.recordBatchList.add(new RecordBatchWithContextWrapper(recordBatchWithContext));
            }
        }

        @Override
        public List<RecordBatchWithContext> recordBatchList() {
            return recordBatchList;
        }

        @Override
        public void free() {
            this.inner.free();
        }
    }

    class RecordBatchWithContextWrapper implements RecordBatchWithContext {
        private final RecordBatchWithContext inner;

        public RecordBatchWithContextWrapper(RecordBatchWithContext inner) {
            this.inner = inner;
        }

        @Override
        public long baseOffset() {
            return inner.baseOffset() - startOffsetInStream;
        }

        @Override
        public long lastOffset() {
            return inner.lastOffset() - startOffsetInStream;
        }

        @Override
        public int count() {
            return inner.count();
        }

        @Override
        public long baseTimestamp() {
            return inner.baseTimestamp();
        }

        @Override
        public Map<String, String> properties() {
            return inner.properties();
        }

        @Override
        public ByteBuffer rawPayload() {
            return inner.rawPayload();
        }
    }
}
