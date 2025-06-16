package kafka.automq.zerozone;

import io.netty.util.concurrent.FastThreadLocal;

public class ZeroZoneThreadLocalContext {

    private static final FastThreadLocal<WriteContext> WRITE_CONTEXT = new FastThreadLocal<>() {
        @Override protected WriteContext initialValue() {
            return new WriteContext();
        }
    };

    public static WriteContext writeContext() {
        return WRITE_CONTEXT.get();
    }


    public static class WriteContext {
        ChannelOffset channelOffset;

        public ChannelOffset channelOffset() {
            return channelOffset;
        }
    }


}
