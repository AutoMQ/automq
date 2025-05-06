package kafka.automq.table.events;

import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class TopicMetric implements Element {
    private long fieldCount;
    private final Schema avroSchema;
    public static final Schema AVRO_SCHEMA = SchemaBuilder.builder().record(TopicMetric.class.getName()).fields()
        .name("fieldCount").type().longType().noDefault()
        .endRecord();
    public static final TopicMetric NOOP = new TopicMetric(0);

    public TopicMetric(Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.fieldCount = 0;
    }

    public TopicMetric(long fieldCount) {
        this.fieldCount = fieldCount;
        this.avroSchema = AVRO_SCHEMA;
    }

    public long fieldCount() {
        return fieldCount;
    }

    @Override
    public void put(int i, Object v) {
        switch (i) {
            case 0:
                this.fieldCount = (long) v;
                return;
            default:
                // ignore the object, it must be from a newer version of the format
        }
    }

    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return fieldCount;
            default:
                throw new UnsupportedOperationException("Unknown field oridinal: " + i);
        }
    }

    @Override
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        TopicMetric metric = (TopicMetric) o;
        return fieldCount == metric.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldCount);
    }

    @Override
    public String toString() {
        return "TopicMetric{" +
            "fieldCount=" + fieldCount +
            '}';
    }
}
