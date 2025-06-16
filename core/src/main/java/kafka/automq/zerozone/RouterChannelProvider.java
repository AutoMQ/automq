package kafka.automq.zerozone;

import org.apache.kafka.controller.stream.RouterChannelEpoch;
import org.apache.kafka.image.loader.MetadataListener;

public interface RouterChannelProvider extends MetadataListener {

    RouterChannel channel();

    RouterChannel readOnlyChannel(int node);

    RouterChannelEpoch epoch();

    void addEpochListener(EpochListener listener);

    interface EpochListener {
        void onNewEpoch(RouterChannelEpoch epoch);
    }
}
