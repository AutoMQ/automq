package kafka.log.es;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import kafka.server.epoch.EpochEntry;

public class ElasticLeaderEpochCheckpointMeta {
    private final int version;
    private List<EpochEntry> entries;

    public ElasticLeaderEpochCheckpointMeta(int version, List<EpochEntry> entries) {
        this.version = version;
        this.entries = entries;
    }

    public byte[] encode() {
        int totalLength = 4 // version
            + 4 // following entries size
            +12 * entries.size(); // all entries
        ByteBuffer buffer = ByteBuffer.allocate(totalLength)
            .putInt(version)
            .putInt(entries.size());
        entries.forEach(entry -> buffer.putInt(entry.epoch()).putLong(entry.startOffset()));
        buffer.flip();
        return buffer.array();
    }

    public static ElasticLeaderEpochCheckpointMeta decode(ByteBuffer buffer) {
        int version = buffer.getInt();
        int entryCount = buffer.getInt();
        List<EpochEntry> entryList = new ArrayList<>(entryCount);
        while(buffer.hasRemaining()) {
            entryList.add(new EpochEntry(buffer.getInt(), buffer.getLong()));
        }
        if (entryList.size() != entryCount) {
            throw new RuntimeException("expect entry count:" + entryCount +  ", decoded " + entryList.size() + " entries");
        }
        return new ElasticLeaderEpochCheckpointMeta(version, entryList);
    }

    public int version() {
        return version;
    }

    public List<EpochEntry> entries() {
        return entries;
    }

    public void setEntries(List<EpochEntry> entries) {
        this.entries = entries;
    }
}
