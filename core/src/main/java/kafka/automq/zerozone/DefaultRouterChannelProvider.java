package kafka.automq.zerozone;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.Time;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultRouterChannelProvider implements RouterChannelProvider {
    public static final String WAL_TYPE = "rc";
    private final int nodeId;
    private final short channelId;
    private final RouterChannel routerChannel;
    private final ObjectStorage objectStorage;
    private final Map<Integer, RouterChannel> routerChannels = new ConcurrentHashMap<>();

    public DefaultRouterChannelProvider(int nodeId, long epoch, BucketURI bucketURI, ObjectStorage objectStorage) {
        this.nodeId = nodeId;
        this.channelId = bucketURI.bucketId();
        this.objectStorage = objectStorage;

        // TODO: wal without fence check
        ObjectWALConfig config = ObjectWALConfig.builder().withNodeId(nodeId).withEpoch(epoch).withOpenMode(OpenMode.READ_WRITE).withType(WAL_TYPE).build();
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
            ObjectWALConfig config = ObjectWALConfig.builder().withNodeId(node).withOpenMode(OpenMode.READ_ONLY).withType(WAL_TYPE).build();
            ObjectWALService wal = new ObjectWALService(Time.SYSTEM, objectStorage, config);
            try {
                wal.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ObjectRouterChannel(nodeId, channelId, wal);
        });
    }
}
