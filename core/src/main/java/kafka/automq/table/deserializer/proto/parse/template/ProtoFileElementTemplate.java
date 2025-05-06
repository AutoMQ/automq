package kafka.automq.table.deserializer.proto.parse.template;

import kafka.automq.table.deserializer.proto.parse.converter.DynamicSchemaConverter;
import kafka.automq.table.deserializer.proto.parse.converter.ProtoFileElementConverter;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;

/**
 * Template implementation for converting ProtoFileElement to DynamicSchema.
 * This class provides the concrete implementation of the template methods
 * specific to ProtoFileElement processing.
 */
public class ProtoFileElementTemplate extends DynamicSchemaTemplate<ProtoFileElement> {
    
    private final ProtoFileElementConverter converter;

    /**
     * Creates a new ProtoFileElementTemplate with a default converter.
     */
    public ProtoFileElementTemplate() {
        this.converter = new ProtoFileElementConverter();
    }

    @Override
    protected DynamicSchemaConverter<ProtoFileElement> getConverter() {
        return converter;
    }
} 