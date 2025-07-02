package kafka.automq.zerozone;

import io.netty.buffer.ByteBuf;

public class ChannelOffset {
    private final ByteBuf buf;

    private ChannelOffset(ByteBuf buf) {
        this.buf = buf;
    }

    public static ChannelOffset of(ByteBuf buf) {
        return new ChannelOffset(buf);
    }

    public int orderHint() {
        return 0;
    }

    public ByteBuf byteBuf() {
        return buf;
    }

}
