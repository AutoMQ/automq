package com.automq.kafka.cloudstorage;

import com.automq.kafka.cloudstorage.api.FastWAL;
import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * [不需要持久化]Position - 表示块存储的物理位置
 * [不需要持久化]RoundNum - 表示正在写第几轮，默认从第 0 轮开始
 * Offset - 表示 WAL 的逻辑位置
 * TrimOffset - 表示 WAL 的逻辑位置，小于此位置的数据已经被删除（实际上传到了 S3）
 * PendingIOWindowMinOffset - Pending IO Window 的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
 * PendingIOWindowNextWriteOffset - Pending IO Window 下一个要写的 Record 对应的 Offset
 * PendingIOWindowMaxSize - 表示 Pending IO Window 的最大大小
 * HeaderMetaMagicCode - 表示 HeaderMeta 的魔数
 * RecordMeta
 * - MagicCode - 表示 RecordMeta 的魔数
 * - RecordBodySize - 表示 Record 的大小
 * - RecordBodyOffset - 表示 Record 的逻辑位置
 * - RecordBodyCRC - 表示 Record body 的 CRC
 * - RecordMetaCRC - 表示 RecordMeta 的 CRC
 */

/**
 * Header 存储结构
 * Header 1 [4K]
 * - HeaderMetaMagicCode [4B]
 * - TrimOffset [8B]
 * - PendingIOWindowMaxSize [4B]
 * - LastWriteTimestamp [8B]
 * - NextWriteOffset [8B]
 * - ShutdownGracefully [1B]
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

    private static int BlockSize = 4096;

    private ScheduledExecutorService scheduledExecutorService;

    private ExecutorService executorService;

    private String devicePath;

    private AsynchronousFileChannel fileChannel;


    public static FastWAL build() {
        EBSFastWAL ebsFastWAL = new EBSFastWAL();
        ebsFastWAL.init();
        return ebsFastWAL;
    }


    private void init() {
        Path path = Paths.get(String.format("%s/ebsTestFile", System.getenv("HOME")));
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
    public void stopAppending() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public AppendResult append(ByteBuffer record, int crc, FutureCallback<AppendResult.CallbackResult> callback) throws OverCapacityException {
        return null;
    }

    @Override
    public List<RecordEntity> read() {
        return null;
    }

    @Override
    public void trim(long offset) {

    }
}

