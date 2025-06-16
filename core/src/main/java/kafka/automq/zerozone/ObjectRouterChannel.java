package kafka.automq.zerozone;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.DefaultRecordOffset;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.buffer.ByteBuf;

public class ObjectRouterChannel implements RouterChannel {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectRouterChannel.class);
    private final AtomicLong mockOffset = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final ObjectWALService wal;
    private final int nodeId;
    private final short channelId;

    private long channelEpoch = 0L;
    private final Queue<Long> channelEpochQueue = new LinkedList<>();
    private final Map<Long, RecordOffset> channelEpoch2LastRecordOffset = new HashMap<>();

    public ObjectRouterChannel(int nodeId, short channelId, ObjectWALService wal) {
        this.nodeId = nodeId;
        this.channelId = channelId;
        this.wal = wal;
    }

    @Override
    public CompletableFuture<AppendResult> append(int targetNodeId, short orderHint, ByteBuf data) {
        StreamRecordBatch record = new StreamRecordBatch(targetNodeId, 0, mockOffset.incrementAndGet(), 1, data);
        try {
            return wal.append(TraceContext.DEFAULT, record).thenApply(walRst -> {
                readLock.lock();
                try {
                    long epoch = this.channelEpoch;
                    ChannelOffset channelOffset = ChannelOffset.of(channelId, orderHint, nodeId, targetNodeId, walRst.recordOffset().buffer());
                    channelEpoch2LastRecordOffset.put(epoch, walRst.recordOffset());
                    return new AppendResult(epoch, channelOffset.byteBuf());
                } finally {
                    readLock.unlock();
                }
            });
        } catch (OverCapacityException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<ByteBuf> get(ByteBuf channelOffset) {
        return wal.get(new DefaultRecordOffset(ChannelOffset.of(channelOffset).walRecordOffset())).thenApply(streamRecordBatch -> {
            ByteBuf payload = streamRecordBatch.getPayload().retainedSlice();
            streamRecordBatch.release();
            return payload;
        });
    }

    @Override
    public void nextEpoch(long epoch) {
        writeLock.lock();
        try {
            if (epoch > this.channelEpoch) {
                this.channelEpoch = epoch;
                this.channelEpochQueue.add(epoch);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void trim(long epoch) {
        writeLock.lock();
        try {
            for (;;) {
                Long channelEpoch = channelEpochQueue.peek();
                if (channelEpoch == null || channelEpoch > epoch) {
                    return;
                }
                channelEpochQueue.poll();
                RecordOffset recordOffset = channelEpoch2LastRecordOffset.remove(epoch);
                wal.trim(recordOffset);
            }
        } finally {
            writeLock.unlock();
        }
    }
}
