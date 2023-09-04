package com.automq.kafka.cloudstorage;

import com.automq.kafka.cloudstorage.api.FastWAL;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

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
 * Header 存储结构， 10s 写一次。
 * Header 1 [4K]
 * - HeaderMetaMagicCode [4B]
 * - ContentSize [8B]
 * - SlidingWindowMaxSize [4B]
 * - TrimOffset [8B]
 * - LastWriteTimestamp [8B]
 * - NextWriteOffset [8B]
 * - ShutdownGracefully [1B]
 * - crc [4B]
 * Header 2 [4K]
 * - Header 2 同 Header 1 数据结构一样，Recover 时，以 LastWriteTimestamp 更大为准。
 * Record 存储结构，每次写都以块大小对齐
 * - MagicCode [4B]
 * - RecordBodySize [4B]
 * - RecordBodyOffset [8B]
 * - RecordBodyCRC [4B]
 * - RecordMetaCRC [4B]
 * - Record [ByteBuffer]
 */

public class EBSFastWAL implements FastWAL {

    static class EBSFastWALBuilder {
        private ScheduledExecutorService scheduledExecutorService;

        private ExecutorService executorService;

        private String devicePath;

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
            return new EBSFastWAL(scheduledExecutorService, executorService, devicePath);
        }
    }

    private static int BlockSize = Integer.parseInt(System.getProperty(//
            "automq.ebswal.blocksize", //
            "4096"));

    private static int AioThreadNums = Integer.parseInt(System.getProperty(//
            "automq.ebswal.aioThreadNums", //
            "8"));

    private static long SlidingWindowUpperLimit = Long.parseLong(System.getProperty(//
            "automq.ebswal.slidingWindowUpperLimit", //
            String.valueOf(1024 * 1024 * 512)));

    private static long SlidingWindowScaleUnit = Long.parseLong(System.getProperty(//
            "automq.ebswal.slidingWindowScaleUnit", //
            String.valueOf(1024 * 1024 * 16)));

    private ScheduledExecutorService scheduledExecutorService;

    private ExecutorService executorService;

    private String devicePath;

    private AsynchronousFileChannel fileChannel;


    public EBSFastWAL(ScheduledExecutorService scheduledExecutorService, ExecutorService executorService, String devicePath) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.executorService = executorService;
        this.devicePath = devicePath;
    }

    private void init() {
        Path path = Paths.get(this.devicePath);
        Set<StandardOpenOption> options = new HashSet<StandardOpenOption>();
        options.add(StandardOpenOption.CREATE);
        options.add(StandardOpenOption.WRITE);
        options.add(StandardOpenOption.READ);
        options.add(StandardOpenOption.DSYNC);
        try {
            fileChannel = AsynchronousFileChannel.open(path, options, executorService, null);
        } catch (IOException e) {
            throw new RuntimeException(String.format("open file [%s] exception", path.getFileName()), e);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdownGracefully() {

    }

    @Override
    public AppendResult append(ByteBuffer record, int crc) throws OverCapacityException {
        return null;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return null;
    }

    @Override
    public void trim(long offset) {

    }
}

