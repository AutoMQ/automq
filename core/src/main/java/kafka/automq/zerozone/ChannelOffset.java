package kafka.automq.zerozone;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ChannelOffset {
    private static final byte MAGIC = (byte) 0x86;
    private static final int CHANNEL_ID_INDEX = 1;
    private static final int ORDER_HINT_INDEX = 3;
    private static final int CHANNEL_OWNER_NODE_ID_INDEX = 5;
    private static final int TARGET_NODE_ID_INDEX = 9;
    private static final int WAL_RECORD_OFFSET_INDEX = 13;

    private final ByteBuf buf;

    private ChannelOffset(ByteBuf buf) {
        this.buf = buf;
    }

    public static ChannelOffset of(ByteBuf buf) {
        return new ChannelOffset(buf);
    }

    public static ChannelOffset of(short channelId, short orderHint, int channelOwnerNodeId, int targetNodeId,
        ByteBuf walRecordOffset) {
        ByteBuf channelOffset = Unpooled.buffer(1 /* magic */ + 2 /* channelId */ + 2 /* orderHint */
            + 4 /* channelOwnerNodeId */ + 4 /* targetNodeId */ + walRecordOffset.readableBytes());
        channelOffset.writeByte(MAGIC);
        channelOffset.writeShort(channelId);
        channelOffset.writeShort(orderHint);
        channelOffset.writeInt(channelOwnerNodeId);
        channelOffset.writeInt(targetNodeId);
        channelOffset.writeBytes(walRecordOffset.duplicate());
        return of(channelOffset);
    }

    public short channelId() {
        return buf.getShort(CHANNEL_ID_INDEX);
    }

    public short orderHint() {
        return buf.getShort(ORDER_HINT_INDEX);
    }

    public int channelOwnerNodeId() {
        return buf.getInt(CHANNEL_OWNER_NODE_ID_INDEX);
    }

    public int targetNodeId() {
        return buf.getInt(TARGET_NODE_ID_INDEX);
    }

    public ByteBuf walRecordOffset() {
        return buf.slice(WAL_RECORD_OFFSET_INDEX, buf.readableBytes() - WAL_RECORD_OFFSET_INDEX);
    }


    public ByteBuf byteBuf() {
        return buf;
    }

}
