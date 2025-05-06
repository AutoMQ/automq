package kafka.automq.table.events;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;

public interface Element extends IndexedRecord, SchemaConstructable {
    Schema UUID_SCHEMA =
        LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().fixed("uuid").size(16));

    static UUID toUuid(GenericData.Fixed fixed) {
        ByteBuffer bb = ByteBuffer.wrap(fixed.bytes());
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);
    }

    static GenericData.Fixed toFixed(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return new GenericData.Fixed(UUID_SCHEMA, bb.array());
    }
}
