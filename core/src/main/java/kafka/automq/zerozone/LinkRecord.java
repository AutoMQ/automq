package kafka.automq.zerozone;

import org.apache.kafka.common.record.MemoryRecords;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class LinkRecord {

    // TODO: encode
    public static ByteBuf encodeLinkRecord(ChannelOffset channelOffset, MemoryRecords records, long streamEpoch) {
        // The MemoryRecords only contains one RecordBatch, cause of produce only send one RecordBatch per partition.
        if (channelOffset == null) {
            return null;
        }
        return Unpooled.buffer();
    }

}
