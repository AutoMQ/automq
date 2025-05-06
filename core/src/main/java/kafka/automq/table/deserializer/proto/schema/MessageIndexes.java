package kafka.automq.table.deserializer.proto.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.utils.ByteUtils;

public class MessageIndexes {
    private static final List<Integer> DEFAULT_INDEX = Collections.singletonList(0);
    private final List<Integer> indexes;

    public MessageIndexes(List<Integer> indexes) {
        this.indexes = new ArrayList<>(indexes);
    }

    public List<Integer> getIndexes() {
        return Collections.unmodifiableList(indexes);
    }

    public byte[] toBytes() {
        if (indexes.size() == 1 && indexes.get(0) == 0) {
            // optimization
            ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(0));
            ByteUtils.writeVarint(0, buffer);
            return buffer.array();
        }
        int size = ByteUtils.sizeOfVarint(indexes.size());
        for (Integer index : indexes) {
            size += ByteUtils.sizeOfVarint(index);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteUtils.writeVarint(indexes.size(), buffer);
        for (Integer index : indexes) {
            ByteUtils.writeVarint(index, buffer);
        }
        return buffer.array();
    }

    public List<Integer> indexes() {
        return indexes;
    }

    private int calculateSize() {
        // 4 bytes for size + 4 bytes per index
        return 4 + (indexes.size() * 4);
    }

    public static MessageIndexes readFrom(ByteBuffer buffer) {
        int size = ByteUtils.readVarint(buffer);
        if (size == 0) {
            return new MessageIndexes(DEFAULT_INDEX);
        }
        List<Integer> indexes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            indexes.add(ByteUtils.readVarint(buffer));
        }
        return new MessageIndexes(indexes);
    }
}

