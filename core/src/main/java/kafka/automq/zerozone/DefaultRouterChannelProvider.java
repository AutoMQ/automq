package kafka.automq.zerozone;

import org.apache.kafka.controller.stream.RouterChannelEpoch;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.MetadataListener;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

public class DefaultRouterChannelProvider implements RouterChannelProvider, MetadataListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRouterChannelProvider.class);
    public static final String WAL_TYPE = "rc";
    private final int nodeId;
    private final short channelId;
    private final RouterChannel routerChannel;
    private final ObjectStorage objectStorage;
    private final Map<Integer, RouterChannel> routerChannels = new ConcurrentHashMap<>();
    private final String clusterId;

    private final List<EpochListener> epochListeners = new CopyOnWriteArrayList<>();
    private volatile RouterChannelEpoch epoch;

    public DefaultRouterChannelProvider(int nodeId, long epoch, BucketURI bucketURI, ObjectStorage objectStorage, String clusterId) {
        this.nodeId = nodeId;
        this.channelId = bucketURI.bucketId();
        this.objectStorage = objectStorage;
        this.clusterId = clusterId;
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId(clusterId).withNodeId(nodeId).withEpoch(epoch).withOpenMode(OpenMode.READ_WRITE).withType(WAL_TYPE).build();
        ObjectWALService wal = new ObjectWALService(Time.SYSTEM, objectStorage, config);
        try {
            wal.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.routerChannel = new ObjectRouterChannel(this.nodeId, channelId, wal);
    }

    @Override
    public RouterChannel channel() {
        return routerChannel;
    }

    @Override
    public RouterChannel readOnlyChannel(int node) {
        if (nodeId == node) {
            return routerChannel;
        }
        return routerChannels.computeIfAbsent(node, nodeId -> {
            ObjectWALConfig config = ObjectWALConfig.builder().withClusterId(clusterId).withNodeId(node).withOpenMode(OpenMode.READ_ONLY).withType(WAL_TYPE).build();
            ObjectWALService wal = new ObjectWALService(Time.SYSTEM, objectStorage, config);
            try {
                wal.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ObjectRouterChannel(nodeId, channelId, wal);
        });
    }

    @Override
    public RouterChannelEpoch epoch() {
        return epoch;
    }

    @Override
    public void addEpochListener(EpochListener listener) {
        epochListeners.add(listener);
    }

    @Override
    public void onChange(MetadataDelta delta, MetadataImage image) {
        ByteBuffer value = image.kv().getValue(RouterChannelEpoch.ROUTER_CHANNEL_EPOCH_KEY);
        if (value == null) {
            return;
        }
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        this.epoch = RouterChannelEpoch.decode(Unpooled.wrappedBuffer(bytes));
        this.routerChannel.nextEpoch(epoch.getCurrent());
        // delay 10s to ensure the delayed replayer can read the data.
        Threads.COMMON_SCHEDULER.schedule(() -> this.routerChannel.trim(epoch.getCommited()), 10, TimeUnit.SECONDS);
        notifyEpochListeners(epoch);

    }

    private void notifyEpochListeners(RouterChannelEpoch epoch) {
        for  (EpochListener listener : epochListeners) {
            try {
                listener.onNewEpoch(epoch);
            } catch (Throwable t) {
                LOGGER.error("Failed to notify epoch listener {}", listener, t);
            }
        }
    }
}
