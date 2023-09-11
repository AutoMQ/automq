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

package kafka.log.s3.wal;

import io.netty.buffer.ByteBuf;
import kafka.log.s3.wal.util.ThreadFactoryImpl;
import kafka.log.s3.wal.util.WALChannel;
import kafka.log.s3.wal.util.WALUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BlockWALService 提供一个无限长的 WAL，基于块设备实现，块设备的容量由应用配置，可能比系统分配的小。
 * 实际块设备大约 10G，采用循环写方式，类似于 RingBuffer，如果调用方没有及时调用 trim，则 append 会抛异常。
 * BlockWALService 不提供运行过程中读操作，仅在异常宕机重启后，调用 start 方法后，可以调用 recover 方法迭代所有未 trim 的数据。
 * --------------------------------------------------------------------------------------------------
 * 块设备上的数据结构描述
 * --------------------------------------------------------------------------------------------------
 * WAL Header 1 [4K] 10s 写一次。
 * 0 - MagicCode [4B] 表示 HeaderMeta 的魔数
 * 1 - Capacity [8B] 表示 BlockDevice 的容量（由应用配置，可能比系统分配的小）
 * 2 - TrimOffset [8B] 表示 WAL 的逻辑位置，小于此位置的数据已经被删除（实际上传到了 S3）
 * 3 - LastWriteTimestamp [8B] 表示上一次写入 WALHeader 的时间戳
 * 4 - SlidingWindowNextWriteOffset [8B] 滑动窗口下一个要写的 Record 对应的 Offset
 * 5 - SlidingWindowStartOffset [8B] 滑动窗口的起始 Offset，此 Offset 之前的数据已经全部成功写入存储设备
 * 6 - SlidingWindowMaxLength [8B] 表示滑动窗口的最大大小
 * 7 - ShutdownType [4B]
 * 8 - crc [4B] 表示 WALHeader 的 CRC
 * WAL Header 1 [4K] 10s 写一次。
 * - Header 2 同 Header 1 数据结构一样，Recover 时，以 LastWriteTimestamp 更大为准。
 * Record Header，每次写都以块大小对齐
 * 0 - MagicCode [4B] 表示 RecordHeader 的魔数
 * 1 - RecordBodyLength [4B] 表示 Record 的长度
 * 2 - RecordBodyOffset [8B] 表示 Record 的逻辑位置
 * 3 - RecordBodyCRC [4B] 表示 Record body 的 CRC
 * 4 - RecordHeaderCRC [4B] 表示 Record Header 的 CRC
 * Record Body，紧接着 Record Header，长度为 RecordBodyLength
 * --------------------------------------------------------------------------------------------------
 */

public class BlockWALService implements WriteAheadLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockWALService.class);
    public static final int RECORD_HEADER_SIZE = 4 + 4 + 8 + 4 + 4;
    public static final int RECORD_HEADER_MAGIC_CODE = 0x87654321;
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345678;
    public static final int WAL_HEADER_SIZE = 4 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4;
    public static final long WAL_HEADER_CAPACITY = WALUtil.BLOCK_SIZE;
    public static final long WAL_HEADER_CAPACITY_DOUBLE = WAL_HEADER_CAPACITY * 2;
    private final AtomicBoolean recoverWALFinished = new AtomicBoolean(false);
    private final AtomicLong writeHeaderRoundTimes = new AtomicLong(0);
    private ScheduledExecutorService scheduledExecutorService;
    private String blockDevicePath;
    private int ioThreadNums;
    private long blockDeviceCapacityWant;
    private WALChannel walChannel;
    private SlidingWindowService slidingWindowService;
    private WALHeaderCoreData walHeaderCoreData;


    private void init() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("block-wal-scheduled-thread-"));

        this.walChannel = WALChannel.WALChannelBuilder.build(blockDevicePath, blockDeviceCapacityWant);

        this.slidingWindowService = new SlidingWindowService(ioThreadNums, walChannel);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> BlockWALService.this.flushWALHeader(
                        this.slidingWindowService.getWindowCoreData().getWindowStartOffset().get(),
                        this.slidingWindowService.getWindowCoreData().getWindowMaxLength().get(),
                        this.slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get(),
                        ShutdownType.UNGRACEFULLY),
                10, 10, TimeUnit.SECONDS);
    }


    private synchronized void flushWALHeader(long windowStartOffset,
                                             long windowMaxLength,
                                             long windowNextWriteOffset,
                                             ShutdownType shutdownType
    ) {
        if (recoverWALFinished.get()) {
            long position = writeHeaderRoundTimes.getAndIncrement() % 2 * WAL_HEADER_CAPACITY;
            try {
                walHeaderCoreData.setLastWriteTimestamp(System.currentTimeMillis()) //
                        .setSlidingWindowStartOffset(windowStartOffset) //
                        .setSlidingWindowMaxLength(windowMaxLength) //
                        .setSlidingWindowNextWriteOffset(windowNextWriteOffset) //
                        .setShutdownType(shutdownType);
                this.walChannel.write(walHeaderCoreData.marshal(), position);
                LOGGER.info("flushWALHeader success, position: {}, walHeader: {}", position, walHeaderCoreData);
            } catch (IOException e) {
                LOGGER.error("flushWALHeader IOException", e);
            }
        }
    }

    private Pair<SlidingWindowService.RecordHeaderCoreData, ByteBuffer> readRecord(WALHeaderCoreData paramWALHeader, long recoverStartOffset) throws ReadRecordException {
        try {
            final ByteBuffer recordHeader = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            final long position = WALUtil.recordOffsetToPosition(recoverStartOffset, paramWALHeader.recordSectionCapacity());
            int read = walChannel.read(recordHeader, position);
            // 检查点：无法读取 RecordHeader
            if (read != RECORD_HEADER_SIZE) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), //
                        String.format("read[%d] != RecordHeaderSize", read) //
                );
            }

            SlidingWindowService.RecordHeaderCoreData readRecordHeader = SlidingWindowService.RecordHeaderCoreData.unmarshal(recordHeader.position(0).limit(RECORD_HEADER_SIZE));
            // 检查点：RecordHeaderMagicCode 不匹配，可能遇到了损坏的数据
            if (readRecordHeader.getMagicCode() != RECORD_HEADER_MAGIC_CODE) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), //
                        String.format("readRecordHeader.getMagicCode()[%d] != RecordHeaderMagicCode[%d]", readRecordHeader.getMagicCode(), RECORD_HEADER_MAGIC_CODE) //
                );
            }

            int recordBodyLength = readRecordHeader.getRecordBodyLength();
            long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
            int recordBodyCRC = readRecordHeader.getRecordBodyCRC();
            int recordHeaderCRC = readRecordHeader.getRecordHeaderCRC();

            // 检查点：RecordHeaderCRC 不匹配，可能遇到了损坏的数据
            if (recordHeaderCRC != WALUtil.crc32(recordHeader.array(), 0, RECORD_HEADER_SIZE - 4)) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), //
                        String.format("recordHeaderCRC[%d] != WALUtil.crc32(recordHeader.array(), 0, RecordHeaderSize - 4)[%d]", recordHeaderCRC, WALUtil.crc32(recordHeader.array(), 0, RECORD_HEADER_SIZE - 4)) //
                );
            }

            // 检查点：RecordBodyLength <= 0，可能遇到了损坏的数据
            if (recordBodyLength <= 0) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), //
                        String.format("recordBodyLength[%d] <= 0", recordBodyLength) //
                );
            }

            ByteBuffer recordBody = ByteBuffer.allocate(recordBodyLength);
            read = walChannel.read(recordBody, WALUtil.recordOffsetToPosition(recordBodyOffset, paramWALHeader.recordSectionCapacity()));
            // 检查点：无法读取 RecordBody
            if (read != recordBodyLength) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength), //
                        String.format("read[%d] != recordBodyLength[%d]", read, recordBodyLength) //
                );
            }

            recordBody.position(0).limit(recordBodyLength);

            // 检查点：RecordBodyCRC 不匹配，可能遇到了损坏的数据
            if (recordBodyCRC != WALUtil.crc32(recordBody.array(), recordBody.position(), recordBody.limit())) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength), //
                        String.format("recordBodyCRC[%d] != WALUtil.crc32(recordBody.array(), recordBody.position(), recordBody.limit())[%d]", recordBodyCRC, WALUtil.crc32(recordBody.array(), recordBody.position(), recordBody.limit())) //
                );
            }

            // 检查点：recordBodyOffset 不匹配，可能遇到了RingBuffer轮转的旧数据
            if (recordBodyOffset != recoverStartOffset + RECORD_HEADER_SIZE) {
                throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), //
                        String.format("recordBodyOffset[%d] != recoverStartOffset[%d] + RecordHeaderSize[%d]", recordBodyOffset, recoverStartOffset, RECORD_HEADER_SIZE) //
                );
            }

            return new ImmutablePair<>(readRecordHeader, recordBody.position(0));
        } catch (Throwable e) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), //
                    String.format("readRecord Exception: %s", e.getMessage()) //
            );
        }
    }


    private WALHeaderCoreData recoverEntireWALAndCorrectWALHeader(WALHeaderCoreData paramWALHeader) {
        // 优雅关闭，不需要纠正 Header
        if (paramWALHeader.getShutdownType().equals(ShutdownType.GRACEFULLY)) {
            return paramWALHeader;
        }

        // 暴力关闭，纠正 Header
        long recoverStartOffset = WALUtil.alignLargeByBlockSize(paramWALHeader.getSlidingWindowStartOffset());
        long recoverRemainingBytes = paramWALHeader.recordSectionCapacity();

        // 总共遇到了几次非法数据
        long meetIllegalRecordTimes = 0;

        do {
            long nextRecoverStartOffset = 0;

            try {
                final Pair<SlidingWindowService.RecordHeaderCoreData, ByteBuffer> resultPair = readRecord(paramWALHeader, recoverStartOffset);
                ByteBuffer body = resultPair.getRight();
                nextRecoverStartOffset = WALUtil.alignLargeByBlockSize(recoverStartOffset + RECORD_HEADER_SIZE + body.limit());
            } catch (ReadRecordException e) {
                nextRecoverStartOffset = e.getJumpNextRecoverOffset();
                LOGGER.info("recoverEntireWALAndCorrectWALHeader ReadRecordException: {}, recoverStartOffset: {}, meetIllegalRecordTimes: {}, recoverRemainingBytes: {}", e.getMessage(), recoverStartOffset, meetIllegalRecordTimes, recoverRemainingBytes);
                meetIllegalRecordTimes++;
            }

            recoverRemainingBytes -= nextRecoverStartOffset - recoverStartOffset;
            recoverStartOffset = nextRecoverStartOffset;
            paramWALHeader.setSlidingWindowStartOffset(nextRecoverStartOffset).setSlidingWindowNextWriteOffset(nextRecoverStartOffset);

            if (meetIllegalRecordTimes == 1) {
                recoverRemainingBytes = paramWALHeader.getSlidingWindowMaxLength();
                LOGGER.info("recoverEntireWALAndCorrectWALHeader first meet illegal record, recoverRemainingBytes: {}", recoverRemainingBytes);
            }
        } while (recoverRemainingBytes > 0);

        return paramWALHeader;
    }

    private void recoverWALHeader() {
        WALHeaderCoreData[] walHeadersRecoveredCoreData = {null, null};
        WALHeaderCoreData walHeaderCoreDataAvailable = null;

        for (int i = 0; i < 2; i++) {
            try {
                final ByteBuffer byteBuffer = ByteBuffer.allocate(WAL_HEADER_SIZE);
                walChannel.read(byteBuffer, i * WAL_HEADER_CAPACITY);
                walHeadersRecoveredCoreData[i] = WALHeaderCoreData.unmarshal(byteBuffer.position(0).limit(WAL_HEADER_SIZE));
                if (walHeaderCoreDataAvailable == null || walHeaderCoreDataAvailable.lastWriteTimestampPos3 < walHeadersRecoveredCoreData[i].lastWriteTimestampPos3) {
                    walHeaderCoreDataAvailable = walHeadersRecoveredCoreData[i];
                }
            } catch (Throwable e) {
                LOGGER.error("recoverWALHeader IOException {}", e.getMessage());
            }
        }

        if (walHeaderCoreDataAvailable != null) {
            LOGGER.info("recoverWALHeader success, walHeader: {}", walHeaderCoreDataAvailable);
            walHeaderCoreData = recoverEntireWALAndCorrectWALHeader(walHeaderCoreDataAvailable);
            LOGGER.info("recoverEntireWALAndCorrectWALHeader success, walHeader: {}", walHeaderCoreData);
            slidingWindowService.resetWindowWhenRecoverOver(//
                    walHeaderCoreData.getSlidingWindowStartOffset(), //
                    walHeaderCoreData.getSlidingWindowNextWriteOffset(), //
                    walHeaderCoreData.getSlidingWindowMaxLength() //
            );
        } else {
            walHeaderCoreData = new WALHeaderCoreData().setCapacity(walChannel.capacity()) //
                    .setTrimOffset(0) //
                    .setLastWriteTimestamp(System.currentTimeMillis()) //
                    .setSlidingWindowNextWriteOffset(0) //
                    .setSlidingWindowStartOffset(0) //
                    .setSlidingWindowMaxLength(0) //
                    .setShutdownType(ShutdownType.UNGRACEFULLY);
            LOGGER.info("recoverWALHeader failed, no available walHeader, Initialize with a complete new wal");
        }
    }

    @Override
    public WriteAheadLog start() throws IOException {
        init();
        walChannel.open();
        recoverWALHeader();
        slidingWindowService.start();
        recoverWALFinished.set(true);
        return this;
    }

    @Override
    public void shutdownGracefully() {
        scheduledExecutorService.shutdown();
        slidingWindowService.shutdown();
        flushWALHeader(slidingWindowService.getWindowCoreData().getWindowStartOffset().get(), //
                slidingWindowService.getWindowCoreData().getWindowMaxLength().get(), //
                slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get(), //
                ShutdownType.GRACEFULLY);
        walChannel.close();
    }

    @Override
    public AppendResult append(ByteBuf buf, int crc) throws OverCapacityException {
        ByteBuffer record = buf.nioBuffer();

        // 生成 crc
        final int recordBodyCRC = 0 == crc ? WALUtil.crc32(record.array(), record.position(), record.limit()) : crc;

        // 计算写入 wal offset
        final long expectedWriteOffset = slidingWindowService.allocateWriteOffset(record.limit(), walHeaderCoreData.getTrimOffset(), walHeaderCoreData.getCapacity() - WAL_HEADER_CAPACITY_DOUBLE);

        // AppendResult
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();

        final AppendResult appendResult = new AppendResultImpl(expectedWriteOffset + RECORD_HEADER_SIZE, recordBodyCRC, record.limit(), appendResultFuture);

        // 生成写 IO 请求，入队执行异步 IO
        slidingWindowService.submitWriteRecordTask(new WriteRecordTask() {
            @Override
            public long writeOffset() {
                return expectedWriteOffset;
            }

            @Override
            public AppendResult appendResult() {
                return appendResult;
            }

            @Override
            public CompletableFuture<AppendResult.CallbackResult> future() {
                return appendResultFuture;
            }

            @Override
            public ByteBuffer recordHeader() {
                SlidingWindowService.RecordHeaderCoreData recordHeaderCoreData = new SlidingWindowService.RecordHeaderCoreData();
                recordHeaderCoreData.setMagicCode(RECORD_HEADER_MAGIC_CODE) //
                        .setRecordBodyLength(record.limit()) //
                        .setRecordBodyOffset(expectedWriteOffset + RECORD_HEADER_SIZE) //
                        .setRecordBodyCRC(recordBodyCRC);
                return recordHeaderCoreData.marshal();
            }

            @Override
            public ByteBuffer recordBody() {
                return record;
            }

            @Override
            public void flushWALHeader(long windowMaxLength) {
                BlockWALService.this.flushWALHeader(
                        slidingWindowService.getWindowCoreData().getWindowStartOffset().get(),
                        windowMaxLength,
                        slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get(),
                        ShutdownType.UNGRACEFULLY
                );
            }
        });


        return appendResult;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return recover(walHeaderCoreData.getTrimOffset());
    }

    private Iterator<RecoverResult> recover(long startOffset) {
        long recoverStartOffset = WALUtil.alignSmallByBlockSize(startOffset);

        LOGGER.info("recover begin, recoverStartOffset: {}, WALHeader: {}", recoverStartOffset, walHeaderCoreData);

        do {
            try {
                final Pair<SlidingWindowService.RecordHeaderCoreData, ByteBuffer> resultPair = readRecord(walHeaderCoreData, recoverStartOffset);
                ByteBuffer body = resultPair.getRight();
                return new RecoverIterator(WALUtil.alignLargeByBlockSize(recoverStartOffset + RECORD_HEADER_SIZE + body.limit()), //
                        resultPair.getLeft(), //
                        resultPair.getRight()//
                );
            } catch (ReadRecordException e) {
                recoverStartOffset = e.getJumpNextRecoverOffset();
            }
        } while (recoverStartOffset < walHeaderCoreData.getSlidingWindowNextWriteOffset());

        LOGGER.info("recover end, recoverStartOffset: {}, WALHeader: {}", recoverStartOffset, walHeaderCoreData);
        return null;
    }

    @Override
    public void trim(long offset) {
        if (offset % WALUtil.BLOCK_SIZE == 0) {
            walHeaderCoreData.setTrimOffset(offset);
        } else {
            walHeaderCoreData.setTrimOffset(offset - RECORD_HEADER_SIZE);
        }
    }

    static class WALHeaderCoreData {
        private final AtomicLong trimOffsetPos2 = new AtomicLong(0);
        private final AtomicLong slidingWindowStartOffsetPos5 = new AtomicLong(0);
        private final AtomicLong slidingWindowNextWriteOffsetPos4 = new AtomicLong(0);
        private final AtomicLong slidingWindowMaxLengthPos6 = new AtomicLong(0);
        private int magicCodePos0 = WAL_HEADER_MAGIC_CODE;
        private long capacityPos1;
        private long lastWriteTimestampPos3 = System.currentTimeMillis();
        private ShutdownType shutdownTypePos7 = ShutdownType.UNGRACEFULLY;
        private int crcPos8;

        public static WALHeaderCoreData unmarshal(ByteBuffer byteBuffer) {
            WALHeaderCoreData walHeaderCoreData = new WALHeaderCoreData();
            walHeaderCoreData.magicCodePos0 = byteBuffer.getInt();
            walHeaderCoreData.capacityPos1 = byteBuffer.getLong();
            walHeaderCoreData.trimOffsetPos2.set(byteBuffer.getLong());
            walHeaderCoreData.lastWriteTimestampPos3 = byteBuffer.getLong();
            walHeaderCoreData.slidingWindowNextWriteOffsetPos4.set(byteBuffer.getLong());
            walHeaderCoreData.slidingWindowStartOffsetPos5.set(byteBuffer.getLong());
            walHeaderCoreData.slidingWindowMaxLengthPos6.set(byteBuffer.getLong());
            walHeaderCoreData.shutdownTypePos7 = ShutdownType.fromCode(byteBuffer.getInt());
            walHeaderCoreData.crcPos8 = byteBuffer.getInt();

            if (walHeaderCoreData.magicCodePos0 != WAL_HEADER_MAGIC_CODE) {
                throw new RuntimeException(String.format("WALHeader MagicCode not match, Recovered: [%d] expect: [%d]", walHeaderCoreData.magicCodePos0, WAL_HEADER_MAGIC_CODE));
            }

            ByteBuffer headerExceptCRC = walHeaderCoreData.marshalHeaderExceptCRC();
            int crc = WALUtil.crc32(headerExceptCRC.array(), 0, WAL_HEADER_SIZE - 4);
            if (crc != walHeaderCoreData.crcPos8) {
                throw new RuntimeException(String.format("WALHeader CRC not match, Recovered: [%d] expect: [%d]", walHeaderCoreData.crcPos8, crc));
            }

            return walHeaderCoreData;
        }

        public long recordSectionCapacity() {
            return capacityPos1 - WAL_HEADER_CAPACITY_DOUBLE;
        }

        public long getCapacity() {
            return capacityPos1;
        }

        public WALHeaderCoreData setCapacity(long capacity) {
            this.capacityPos1 = capacity;
            return this;
        }

        public long getSlidingWindowStartOffset() {
            return slidingWindowStartOffsetPos5.get();
        }

        public WALHeaderCoreData setSlidingWindowStartOffset(long slidingWindowStartOffset) {
            this.slidingWindowStartOffsetPos5.set(slidingWindowStartOffset);
            return this;
        }

        public long getTrimOffset() {
            return trimOffsetPos2.get();
        }

        public WALHeaderCoreData setTrimOffset(long trimOffset) {
            this.trimOffsetPos2.set(trimOffset);
            return this;
        }

        public long getLastWriteTimestamp() {
            return lastWriteTimestampPos3;
        }

        public WALHeaderCoreData setLastWriteTimestamp(long lastWriteTimestamp) {
            this.lastWriteTimestampPos3 = lastWriteTimestamp;
            return this;
        }

        public long getSlidingWindowNextWriteOffset() {
            return slidingWindowNextWriteOffsetPos4.get();
        }

        public WALHeaderCoreData setSlidingWindowNextWriteOffset(long slidingWindowNextWriteOffset) {
            this.slidingWindowNextWriteOffsetPos4.set(slidingWindowNextWriteOffset);
            return this;
        }

        public long getSlidingWindowMaxLength() {
            return slidingWindowMaxLengthPos6.get();
        }

        public WALHeaderCoreData setSlidingWindowMaxLength(long slidingWindowMaxLength) {
            this.slidingWindowMaxLengthPos6.set(slidingWindowMaxLength);
            return this;
        }

        public ShutdownType getShutdownType() {
            return shutdownTypePos7;
        }

        public WALHeaderCoreData setShutdownType(ShutdownType shutdownType) {
            this.shutdownTypePos7 = shutdownType;
            return this;
        }

        @Override
        public String toString() {
            return "WALHeader{" + "magicCode=" + magicCodePos0 + ", capacity=" + capacityPos1 + ", trimOffset=" + trimOffsetPos2 + ", lastWriteTimestamp=" + lastWriteTimestampPos3 + ", nextWriteOffset=" + slidingWindowNextWriteOffsetPos4 + ", slidingWindowMaxLength=" + slidingWindowMaxLengthPos6 + ", shutdownType=" + shutdownTypePos7 + ", crc=" + crcPos8 + '}';
        }

        private ByteBuffer marshalHeaderExceptCRC() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(WAL_HEADER_SIZE);
            byteBuffer.putInt(magicCodePos0);
            byteBuffer.putLong(capacityPos1);
            byteBuffer.putLong(trimOffsetPos2.get());
            byteBuffer.putLong(lastWriteTimestampPos3);
            byteBuffer.putLong(slidingWindowNextWriteOffsetPos4.get());
            byteBuffer.putLong(slidingWindowStartOffsetPos5.get());
            byteBuffer.putLong(slidingWindowMaxLengthPos6.get());
            byteBuffer.putInt(shutdownTypePos7.getCode());

            return byteBuffer;
        }

        ByteBuffer marshal() {
            ByteBuffer byteBuffer = marshalHeaderExceptCRC();
            this.crcPos8 = WALUtil.crc32(byteBuffer.array(), 0, WAL_HEADER_SIZE - 4);
            byteBuffer.putInt(crcPos8);
            return byteBuffer.position(0);
        }
    }

    static class BlockWALServiceBuilder {
        private int ioThreadNums = Integer.parseInt(System.getProperty(//
                "automq.ebswal.ioThreadNums", //
                "8"));
        private String blockDevicePath;
        private long blockDeviceCapacityWant = 0;

        public static BlockWALServiceBuilder build() {
            return new BlockWALServiceBuilder();
        }

        public BlockWALServiceBuilder setIoThreadNums(int ioThreadNums) {
            this.ioThreadNums = ioThreadNums;
            return this;
        }

        public BlockWALServiceBuilder setBlockDevicePath(String blockDevicePath) {
            this.blockDevicePath = blockDevicePath;
            return this;
        }

        public BlockWALServiceBuilder setBlockDeviceCapacityWant(long blockDeviceCapacityWant) {
            this.blockDeviceCapacityWant = blockDeviceCapacityWant;
            return this;
        }


        public BlockWALService createBlockWALService() {
            BlockWALService blockWALService = new BlockWALService();
            blockWALService.blockDevicePath = this.blockDevicePath;
            blockWALService.ioThreadNums = this.ioThreadNums;
            blockWALService.blockDeviceCapacityWant = this.blockDeviceCapacityWant;
            return blockWALService;
        }
    }

    static class AppendResultImpl implements AppendResult {
        private final long recordBodyOffset;
        private final int recordBodyCRC;
        private final int length;
        private final CompletableFuture<CallbackResult> future;

        public AppendResultImpl(long recordBodyOffset, int recordBodyCRC, int length, CompletableFuture<CallbackResult> future) {
            this.recordBodyOffset = recordBodyOffset;
            this.recordBodyCRC = recordBodyCRC;
            this.length = length;
            this.future = future;
        }

        @Override
        public long recordBodyOffset() {
            return recordBodyOffset;
        }

        @Override
        public int recordBodyCRC() {
            return recordBodyCRC;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public CompletableFuture<CallbackResult> future() {
            return future;
        }

        @Override
        public String toString() {
            return "AppendResultImpl{" + "recordBodyOffset=" + recordBodyOffset + ", recordBodyCRC=" + recordBodyCRC + ", length=" + length + '}';
        }
    }

    static class RecoverResultImpl implements RecoverResult {
        private final ByteBuffer record;
        private long recordBodyOffset;
        private int length;

        public RecoverResultImpl(ByteBuffer record, long recordBodyOffset, int length) {
            this.record = record;
        }

        @Override
        public ByteBuffer record() {
            return record;
        }

        @Override
        public long recordBodyOffset() {
            return recordBodyOffset;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public String toString() {
            return "RecoverResultImpl{" +
                    "record=" + record +
                    ", recordBodyOffset=" + recordBodyOffset +
                    ", length=" + length +
                    '}';
        }
    }

    class RecoverIterator implements Iterator<RecoverResult> {
        long nextRecoverOffset;
        SlidingWindowService.RecordHeaderCoreData lastRecordHeader;
        ByteBuffer lastRecordBody;

        public RecoverIterator(long nextRecoverOffset, SlidingWindowService.RecordHeaderCoreData lastRecordHeader, ByteBuffer lastRecordBody) {
            this.nextRecoverOffset = nextRecoverOffset;
            this.lastRecordHeader = lastRecordHeader;
            this.lastRecordBody = lastRecordBody;
        }

        @Override
        public boolean hasNext() {
            return lastRecordBody != null;
        }

        @Override
        public RecoverResult next() {
            if (!hasNext()) return null;

            SlidingWindowService.RecordHeaderCoreData nextRecordHeader = null;
            ByteBuffer nextRecordBody = null;

            // 尝试读取下一条记录
            do {
                try {
                    final Pair<SlidingWindowService.RecordHeaderCoreData, ByteBuffer> resultPair = readRecord(walHeaderCoreData, nextRecoverOffset);
                    nextRecordHeader = resultPair.getLeft();
                    nextRecordBody = resultPair.getRight();
                    nextRecoverOffset = WALUtil.alignLargeByBlockSize(nextRecoverOffset + RECORD_HEADER_SIZE + nextRecordBody.limit());
                    break;
                } catch (ReadRecordException e) {
                    nextRecoverOffset = e.getJumpNextRecoverOffset();
                }
            } while (nextRecoverOffset < walHeaderCoreData.getSlidingWindowNextWriteOffset());

            // 返回之前读取的记录
            RecoverResult recoverResult = new RecoverResultImpl(lastRecordBody, lastRecordHeader.getRecordBodyOffset(), lastRecordBody.limit());

            // 保存下一条记录
            if (nextRecordBody != null) {
                lastRecordHeader = nextRecordHeader;
                lastRecordBody = nextRecordBody;
            } else {
                lastRecordHeader = null;
                lastRecordBody = null;
            }

            return recoverResult;
        }
    }

    static class ReadRecordException extends Exception {
        long jumpNextRecoverOffset;

        public ReadRecordException(long offset, String message) {
            super(message);
            this.jumpNextRecoverOffset = offset;
        }

        public long getJumpNextRecoverOffset() {
            return jumpNextRecoverOffset;
        }
    }
}

