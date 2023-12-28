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

package kafka.log.streamaspect;

import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.utils.FutureUtil;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class DefaultElasticStreamSlice implements ElasticStreamSlice {
    /**
     * the real start offset of this segment in the stream.
     */
    private final long startOffsetInStream;
    private final Stream stream;
    /**
     * next relative offset to be appended to this segment.
     */
    private long nextOffset;
    private boolean sealed = false;
    /**
     * executor service for async operations.
     */
    private final ExecutorService executorService;

    public DefaultElasticStreamSlice(Stream stream, SliceRange sliceRange, ExecutorService executorService) {
        this.stream = stream;
        long streamNextOffset = stream.nextOffset();
        if (sliceRange.start() == Offsets.NOOP_OFFSET) {
            // new stream slice
            this.startOffsetInStream = streamNextOffset;
            sliceRange.start(startOffsetInStream);
            this.nextOffset = 0L;
        } else if (sliceRange.end() == Offsets.NOOP_OFFSET) {
            // unsealed stream slice
            this.startOffsetInStream = sliceRange.start();
            this.nextOffset = streamNextOffset - startOffsetInStream;
        } else {
            // sealed stream slice
            this.startOffsetInStream = sliceRange.start();
            this.nextOffset = sliceRange.end() - startOffsetInStream;
            this.sealed = true;
        }
        this.executorService = executorService;
    }

    @Override
    public CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
        if (sealed) {
            return FutureUtil.failedFuture(new IllegalStateException("stream segment " + this + " is sealed"));
        }
        nextOffset += recordBatch.count();
        return stream.append(context, recordBatch).thenApply(AppendResultWrapper::new);
    }

    @Override
    public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint) {
        long fixedStartOffset = Utils.max(startOffset, 0);
        return stream.fetch(context, startOffsetInStream + fixedStartOffset, startOffsetInStream + endOffset, maxBytesHint)
                .thenApplyAsync(FetchResultWrapper::new, executorService);
    }

    @Override
    public long nextOffset() {
        return nextOffset;
    }

    @Override
    public long startOffsetInStream() {
        return startOffsetInStream;
    }

    @Override
    public SliceRange sliceRange() {
        if (sealed) {
            return SliceRange.of(startOffsetInStream, startOffsetInStream + nextOffset);
        } else {
            return SliceRange.of(startOffsetInStream, Offsets.NOOP_OFFSET);
        }
    }

    @Override
    public void seal() {
        this.sealed = true;
    }

    @Override
    public Stream stream() {
        return stream;
    }

    @Override
    public String toString() {
        return "DefaultElasticStreamSlice{" +
                "startOffsetInStream=" + startOffsetInStream +
                ", stream=[id=" + stream.streamId() + ", startOffset=" + stream.startOffset() + ", nextOffset=" + stream.nextOffset() + "]" +
                ", nextOffset=" + nextOffset +
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
            this.recordBatchList = fetchResult.recordBatchList().stream().map(RecordBatchWithContextWrapper::new).collect(Collectors.toList());
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
