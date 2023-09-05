package com.automq.kafka.cloudstorage;

import com.automq.kafka.cloudstorage.api.FastWAL;
import com.automq.kafka.cloudstorage.util.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * [不需要持久化]Position - 表示块存储的物理位置
 * [不需要持久化]RoundNum - 表示正在写第几轮，默认从第 0 轮开始
 * Offset - 表示 WAL 的逻辑位置
 * TrimOffset - 表示 WAL 的逻辑位置，小于此位置的数据已经被删除（实际上传到了 S3）
 * SlidingWindowMinOffset - 滑动窗口的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
 * SlidingWindowNextWriteOffset - 滑动窗口下一个要写的 Record 对应的 Offset
 * SlidingWindowMaxSize - 表示 滑动窗口的最大大小
 * HeaderMetaMagicCode - 表示 HeaderMeta 的魔数
 * RecordMeta
 * - MagicCode - 表示 RecordMeta 的魔数
 * - RecordBodySize - 表示 Record 的大小
 * - RecordBodyOffset - 表示 Record 的逻辑位置
 * - RecordBodyCRC - 表示 Record body 的 CRC
 * - RecordMetaCRC - 表示 RecordMeta 的 CRC
 */

/**
 * WAL Header 1 10s 写一次。
 * WAL Header 1 [4K]
 * - MagicCode [4B]
 * - ContentSize [8B]
 * - SlidingWindowMaxSize [8B]
 * - TrimOffset [8B]
 * - LastWriteTimestamp [8B]
 * - NextWriteOffset [8B]
 * - ShutdownGracefully [1B]
 * - crc [4B]
 * WAL Header 2 [4K]
 * - Header 2 同 Header 1 数据结构一样，Recover 时，以 LastWriteTimestamp 更大为准。
 * Record Header，每次写都以块大小对齐
 * - MagicCode [4B]
 * - RecordBodyLength [4B]
 * - RecordBodyOffset [8B]
 * - RecordBodyCRC [4B]
 * - RecordHeaderCRC [4B]
 * - Record [ByteBuffer]
 */

public class EBSFastWAL implements FastWAL {
    private static final Logger LOGGER = LoggerFactory.getLogger(EBSFastWAL.class.getSimpleName());
    private static final int WALHeaderMagicCode = 0x12345678;
    private static final int WALHeaderSize = 4 + 8 + 8 + 8 + 8 + 8 + 1 + 4;
    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService executorService;
    private String devicePath;
    private int aioThreadNums;
    private long contentSize;
    private AsynchronousFileChannel fileChannel;
    private final AtomicLong trimOffset = new AtomicLong(0);
    private SlidingWindowService slidingWindowService;
    private final AtomicBoolean recoverWALFinished = new AtomicBoolean(false);

    public static int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    private void init() {
        Path path = Paths.get(this.devicePath);
        Set<StandardOpenOption> options = new HashSet<StandardOpenOption>();
        options.add(StandardOpenOption.CREATE);
        options.add(StandardOpenOption.WRITE);
        options.add(StandardOpenOption.READ);
        options.add(StandardOpenOption.DSYNC);

        // 如果没有指定线程池，则创建一个
        if (this.executorService == null) {
            this.executorService = Executors.newFixedThreadPool(this.aioThreadNums, new ThreadFactoryImpl("ebswal-aio-thread"));
        }

        try {
            fileChannel = AsynchronousFileChannel.open(path, options, executorService, null);
        } catch (IOException e) {
            throw new RuntimeException(String.format("open file [%s] exception", path.getFileName()), e);
        }

        // 如果没有指定定时任务线程，则创建一个
        if (this.scheduledExecutorService == null) {
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ebswal-scheduled-thread"));
        }

        // 定时写 WAL Header
        this.scheduledExecutorService.scheduleAtFixedRate(EBSFastWAL.this::flushWALHeader, 10, 10, TimeUnit.SECONDS);
    }

    private void flushWALHeader() {
        final byte ShutdownGracefullyFalse = 0;
        final byte ShutdownGracefullyTrue = 1;
        // TODO 定时写 Header
        if (recoverWALFinished.get()) {
            // TODO

            ByteBuffer walHeaderBuffer = ByteBuffer.allocate(WALHeaderSize);

            // MagicCode
            walHeaderBuffer.putInt(WALHeaderMagicCode);
            // ContentSize
            walHeaderBuffer.putLong(contentSize);
            // SlidingWindowMaxSize
            walHeaderBuffer.putLong(slidingWindowService.getSlidingWindowMaxSize());
            // TrimOffset
            walHeaderBuffer.putLong(trimOffset.get());
            // LastWriteTimestamp
            walHeaderBuffer.putLong(System.currentTimeMillis());
            // NextWriteOffset
            walHeaderBuffer.putLong(slidingWindowService.getSlidingWindowNextWriteOffset());
            // ShutdownGracefully
            walHeaderBuffer.put(ShutdownGracefullyFalse);
            // crc
            walHeaderBuffer.putInt(crc32(walHeaderBuffer.array(), 0, WALHeaderSize - 4));

            walHeaderBuffer.flip();

            // TODO 计算写入第几个 WAL Header
            long position = 0;
            writeFileSynchronously(fileChannel, walHeaderBuffer, position);
        }
    }

    public void writeFileSynchronously(final AsynchronousFileChannel fileChannel, final ByteBuffer buffer, long position) {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger remainingBytes = new AtomicInteger(buffer.limit());
        fileChannel.write(buffer, position, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                remainingBytes.set(remainingBytes.intValue() - result.intValue());

                if (remainingBytes.intValue() > 0) {
                    buffer.position(buffer.limit() - remainingBytes.intValue());

                    fileChannel.write(buffer, position + buffer.position(), attachment, this);
                } else {
                    latch.countDown();
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                LOGGER.error("writeFileSynchronously failed", exc);
            }
        });

        try {
            latch.wait(10000);
        } catch (InterruptedException e) {
            LOGGER.error("writeFileSynchronously InterruptedException", e);
        }
    }

    @Override
    public void start() {
        // TODO WAL Header 要全部扫描数据才能还原。
        recoverWALFinished.set(true);
    }

    @Override
    public void shutdownGracefully() {


    }

    @Override
    public AppendResult append(ByteBuffer record, int crc) throws OverCapacityException {
        // 生成 crc
        int recordBodyCRC = crc;
        if (recordBodyCRC == 0) {
            recordBodyCRC = crc32(record.array(), record.position(), record.limit());
        }

        // 计算写入 wal offset
        long expectedWriteOffset = slidingWindowService.allocateWriteOffset(record.limit(), trimOffset.get());


        AppendResult appendResult = new AppendResult() {
            @Override
            public long walOffset() {
                return 0;
            }

            @Override
            public int length() {
                return 0;
            }

            @Override
            public CompletableFuture<CallbackResult> future() {
                return null;
            }
        };

        return null;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return null;
    }

    @Override
    public void trim(long offset) {
        trimOffset.set(offset);
    }

    static class EBSFastWALBuilder {
        private ScheduledExecutorService scheduledExecutorService;

        private ExecutorService executorService;


        private int aioThreadNums = Integer.parseInt(System.getProperty(//
                "automq.ebswal.aioThreadNums", //
                "8"));

        private String devicePath;

        private final long contentSize = 0;

        public EBSFastWALBuilder setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        public EBSFastWALBuilder setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public EBSFastWALBuilder setDevicePath(String devicePath) {
            this.devicePath = devicePath;
            return this;
        }

        public EBSFastWAL createEBSFastWAL() {
            EBSFastWAL ebsFastWAL = new EBSFastWAL();
            ebsFastWAL.scheduledExecutorService = this.scheduledExecutorService;
            ebsFastWAL.executorService = this.executorService;
            ebsFastWAL.devicePath = this.devicePath;
            ebsFastWAL.aioThreadNums = this.aioThreadNums;
            ebsFastWAL.contentSize = this.contentSize;
            return ebsFastWAL;
        }

        public EBSFastWALBuilder setAioThreadNums(int aioThreadNums) {
            this.aioThreadNums = aioThreadNums;
            return this;
        }
    }
}

