package kafka.automq.table.transformer;

import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;

public class LazyRegistrySchemaConvert implements Converter {
    private final Supplier<Converter> converterSupplier;
    private volatile Converter delegate;

    public LazyRegistrySchemaConvert(Supplier<Converter> converterSupplier) {
        this.converterSupplier = converterSupplier;
    }

    protected Converter getDelegate() {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    delegate = converterSupplier.get();
                    if (delegate == null) {
                        throw new IllegalStateException("Converter supplier returned null");
                    }
                }
            }
        }
        return delegate;
    }

    @Override
    public Record convert(org.apache.kafka.common.record.Record record) {
        return getDelegate().convert(record);
    }

    @Override
    public void tableSchema(Schema tableSchema) {
        getDelegate().tableSchema(tableSchema);
    }

    @Override
    public SchemaState currentSchema() {
        return getDelegate().currentSchema();
    }

    @Override
    public long fieldCount() {
        return getDelegate().fieldCount();
    }
}
