package kafka.automq.zerozone;

import org.apache.kafka.controller.stream.RouterChannelEpoch;

public interface RouterChannelProvider {

    RouterChannel channel();

    RouterChannel readOnlyChannel(int node);

    RouterChannelEpoch epoch();

    void addEpochListener(EpochListener listener);

    interface EpochListener {
        void onNewEpoch(RouterChannelEpoch epoch);
    }
}
