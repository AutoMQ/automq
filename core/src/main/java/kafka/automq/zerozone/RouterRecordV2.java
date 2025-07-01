package kafka.automq.zerozone;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RouterRecordV2 {
    private static final short MAGIC = 0x02;
    private final int nodeId;
    private final List<ByteBuf> channelOffsets;

    public RouterRecordV2(int nodeId, List<ByteBuf> channelOffsets) {
        this.nodeId = nodeId;
        this.channelOffsets = channelOffsets;
    }

    public int nodeId() {
        return nodeId;
    }

    public List<ByteBuf> channelOffsets() {
        return channelOffsets;
    }

    public ByteBuf encode() {
        int size = 1 /* magic */ + 4 /* nodeId */ + channelOffsets.stream().mapToInt(buf -> buf.readableBytes() + 2).sum();
        ByteBuf buf = Unpooled.buffer(size);
        buf.writeByte(MAGIC);
        buf.writeInt(nodeId);
        channelOffsets.forEach(channelOffset -> {
            buf.writeShort(channelOffset.readableBytes());
            buf.writeBytes(channelOffset.duplicate());
        });
        return buf;
    }

    public static RouterRecordV2 decode(ByteBuf buf) {
        buf = buf.slice();
        byte magic = buf.readByte();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        int nodeId = buf.readInt();
        List<ByteBuf> channelOffsets = new ArrayList<>(buf.readableBytes() / 16);
        while (buf.readableBytes() > 0) {
            short size = buf.readShort();
            ByteBuf channelOffset = Unpooled.buffer(size);
            buf.readBytes(channelOffset);
        }
        return new RouterRecordV2(nodeId, channelOffsets);
    }

}
