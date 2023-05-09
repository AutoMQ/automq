package kafka.log.es;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * refers to all valid snapshot files
 */
public class ElasticPartitionProducerSnapshotsMeta {
    public static final ElasticPartitionProducerSnapshotsMeta EMPTY = new ElasticPartitionProducerSnapshotsMeta(new HashSet<>());
    private Set<Long> snapshots;

    ElasticPartitionProducerSnapshotsMeta(Set<Long> snapshots) {
        this.snapshots = snapshots;
    }

    public Set<Long> getSnapshots() {
        return snapshots;
    }

    public void remove(Long offset) {
        snapshots.remove(offset);
    }

    public void add(Long offset) {
        snapshots.add(offset);
    }

    public ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(snapshots.size() * 8);
        snapshots.forEach(item -> {
            if (item != null) {
                buffer.putLong(item);
            }
        });
        buffer.flip();
        return buffer;
    }

    public ElasticPartitionProducerSnapshotsMeta decode(ByteBuffer buffer) {
        Set<Long> snapshots = new HashSet<>();
        while (buffer.hasRemaining()) {
            snapshots.add(buffer.getLong());
        }
        return new ElasticPartitionProducerSnapshotsMeta(snapshots);
    }

    public boolean isEmpty() {
        return snapshots.isEmpty();
    }
}
