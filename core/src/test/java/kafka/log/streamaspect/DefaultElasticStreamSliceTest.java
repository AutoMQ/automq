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

import com.automq.stream.DefaultRecordBatch;
import com.automq.stream.api.RecordBatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(60)
@Tag("S3Unit")
/** Tests bounded logical sealing and append visibility for stream-backed slices. */
public class DefaultElasticStreamSliceTest {

    /**
     * Given an active slice, when it is sealed at an earlier exclusive offset, then its logical offsets and metadata
     * use the supplied end.
     */
    @Test
    public void testSealActiveSliceAtBoundedEnd() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        stream.append(recordBatch(4)).join();
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(4L, Offsets.NOOP_OFFSET));
        slice.append(recordBatch(2)).join();
        slice.append(recordBatch(3)).join();

        slice.seal(2L);

        assertEquals(2L, slice.nextOffset());
        assertEquals(2L, slice.confirmOffset());
        assertEquals(4L, slice.sliceRange().start());
        assertEquals(6L, slice.sliceRange().end());
        assertEquals(9L, stream.nextOffset());
    }

    /**
     * Given an active slice, when it is bounded at its current end, then that end is accepted and fixed.
     */
    @Test
    public void testSealActiveSliceAtCurrentEnd() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0L, Offsets.NOOP_OFFSET));
        slice.append(recordBatch(5)).join();

        slice.seal(5L);

        assertEquals(5L, slice.nextOffset());
        assertEquals(5L, slice.confirmOffset());
        assertEquals(5L, slice.sliceRange().end());
    }

    /**
     * Given a sealed slice, when it is sealed again at an earlier end, then the immutable view is shortened.
     */
    @Test
    public void testShortenSealedSlice() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        stream.append(recordBatch(5)).join();
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0L, 5L));

        slice.seal(2L);

        assertEquals(2L, slice.nextOffset());
        assertEquals(2L, slice.confirmOffset());
        assertEquals(2L, slice.sliceRange().end());
        assertEquals(5L, stream.nextOffset());
    }

    /**
     * Given an active or sealed slice, when an invalid numeric end is supplied, then sealing fails without changing
     * the slice state.
     */
    @Test
    public void testRejectInvalidEndWithoutChangingState() {
        MemoryClient.StreamImpl activeStream = new MemoryClient.StreamImpl(1L);
        ElasticStreamSlice activeSlice = new DefaultElasticStreamSlice(
            activeStream, SliceRange.of(0L, Offsets.NOOP_OFFSET));
        activeSlice.append(recordBatch(5)).join();

        assertThrows(IllegalArgumentException.class, () -> activeSlice.seal(-1L));
        assertThrows(IllegalArgumentException.class, () -> activeSlice.seal(6L));
        assertEquals(5L, activeSlice.nextOffset());
        assertEquals(Offsets.NOOP_OFFSET, activeSlice.sliceRange().end());
        activeSlice.append(recordBatch(1)).join();

        ElasticStreamSlice sealedSlice = new DefaultElasticStreamSlice(activeStream, SliceRange.of(0L, 5L));
        assertThrows(IllegalArgumentException.class, () -> sealedSlice.seal(6L));
        assertEquals(5L, sealedSlice.nextOffset());
        assertEquals(5L, sealedSlice.sliceRange().end());
    }

    /**
     * Given a bounded sealed slice, when append is attempted, then the append fails and the physical stream is not
     * changed.
     */
    @Test
    public void testRejectAppendAfterBoundedSeal() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0L, Offsets.NOOP_OFFSET));
        slice.append(recordBatch(5)).join();
        slice.seal(2L);

        CompletionException exception = assertThrows(CompletionException.class,
            () -> slice.append(recordBatch(1)).join());

        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals(5L, stream.nextOffset());
    }

    /**
     * Given records beyond a sealed logical end, when the slice is fetched, then only records inside the sealed view
     * are visible.
     */
    @Test
    public void testFetchDoesNotExposeRecordsBeyondBoundedEnd() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0L, Offsets.NOOP_OFFSET));
        slice.append(recordBatch(2)).join();
        slice.append(recordBatch(3)).join();
        slice.seal(2L);

        assertEquals(1, slice.fetch(0L, 5L, 1024).join().recordBatchList().size());
        assertEquals(0, slice.fetch(2L, 5L, 1024).join().recordBatchList().size());
    }

    /**
     * Given a bounded range emitted by a sealed slice, when it is reloaded after the physical stream advances, then
     * the same immutable logical view is reconstructed.
     */
    @Test
    public void testReloadBoundedSliceRange() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0L, Offsets.NOOP_OFFSET));
        slice.append(recordBatch(2)).join();
        slice.append(recordBatch(3)).join();
        slice.seal(2L);
        SliceRange sealedRange = slice.sliceRange();
        stream.append(recordBatch(4)).join();

        ElasticStreamSlice reloaded = new DefaultElasticStreamSlice(stream, sealedRange);

        assertEquals(2L, reloaded.nextOffset());
        assertEquals(2L, reloaded.confirmOffset());
        assertEquals(2L, reloaded.sliceRange().end());
        assertEquals(1, reloaded.fetch(0L, 9L, 1024).join().recordBatchList().size());
        assertEquals(9L, stream.nextOffset());
    }

    /**
     * Given the compatibility seal operation, when it seals an active slice, then it fixes the current relative end
     * and cannot later expand with the underlying stream.
     */
    @Test
    public void testSealWithoutEndUsesCurrentEnd() {
        MemoryClient.StreamImpl stream = new MemoryClient.StreamImpl(1L);
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0L, Offsets.NOOP_OFFSET));
        slice.append(recordBatch(5)).join();

        slice.seal();
        stream.append(recordBatch(2)).join();
        slice.seal();

        assertEquals(5L, slice.nextOffset());
        assertEquals(5L, slice.confirmOffset());
        assertEquals(5L, slice.sliceRange().end());
        assertEquals(7L, stream.nextOffset());
    }

    private static RecordBatch recordBatch(int count) {
        return new DefaultRecordBatch(count, 0L, Collections.emptyMap(), ByteBuffer.allocate(count));
    }
}
