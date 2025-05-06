package kafka.automq.table.events;

import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class WorkerOffset implements Element {
    private int partition;
    private int epoch;
    private long offset;
    private final Schema avroSchema;

    public static final Schema AVRO_SCHEMA = SchemaBuilder.builder()
        .record(WorkerOffset.class.getName())
        .fields()
        .name("partition")
        .type().intType().noDefault()
        .name("epoch")
        .type().intType().noDefault()
        .name("offset")
        .type().longType().noDefault()
        .endRecord();

    public WorkerOffset(Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public WorkerOffset(int partition, int epoch, long offset) {
        this.partition = partition;
        this.epoch = epoch;
        this.offset = offset;
        this.avroSchema = AVRO_SCHEMA;
    }

    public int partition() {
        return partition;
    }

    public int epoch() {
        return epoch;
    }

    public long offset() {
        return offset;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.partition = (int) v;
                return;
            case 1:
                this.epoch = (int) v;
                return;
            case 2:
                this.offset = (long) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return partition;
            case 1:
                return epoch;
            case 2:
                return offset;
            default:
                throw new UnsupportedOperationException("Unknown field ordinal: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public String toString() {
        return "WorkerOffset{" +
            "partition=" + partition +
            ", epoch=" + epoch +
            ", offset=" + offset +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        WorkerOffset offset1 = (WorkerOffset) o;
        return partition == offset1.partition && epoch == offset1.epoch && offset == offset1.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, epoch, offset);
    }
}
