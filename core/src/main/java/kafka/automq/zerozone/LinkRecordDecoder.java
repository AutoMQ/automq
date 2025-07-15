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
        LinkRecord linkRecord = LinkRecord.decode(src.getPayload());
        ChannelOffset channelOffset = linkRecord.channelOffset();
        RouterChannel routerChannel = channelProvider.readOnlyChannel(channelOffset.channelOwnerNodeId());
        return routerChannel.get(channelOffset.byteBuf()).thenApply(buf -> {
            try {
                ZoneRouterProduceRequest req = ZoneRouterPackReader.decodeDataBlock(buf).get(0);
                MemoryRecords records = (MemoryRecords) (req.data().topicData().iterator().next()
                    .partitionData().iterator().next()
                    .records());
                MutableRecordBatch recordBatch = records.batches().iterator().next();
                recordBatch.setLastOffset(linkRecord.lastOffset());
                recordBatch.setMaxTimestamp(linkRecord.timestampType(), linkRecord.maxTimestamp());
                recordBatch.setPartitionLeaderEpoch(linkRecord.partitionLeaderEpoch());
                return new StreamRecordBatch(src.getStreamId(), src.getEpoch(), src.getBaseOffset(),
                    -src.getCount(), Unpooled.wrappedBuffer(records.buffer()));
            } finally {
                src.release();
                buf.release();
            }
        });
    }
}
