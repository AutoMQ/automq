package kafka.automq.table.deserializer.proto;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Map;

public class ProtobufSchemaProvider extends AbstractSchemaProvider {

    @Override
    public String schemaType() {
        return "PROTOBUF";
    }

    @Override
    public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
        Map<String, String> resolveReferences = resolveReferences(schema);
        return new CustomProtobufSchema(
            null,
            schema.getVersion(),
            schema.getMetadata(),
            schema.getRuleSet(),
            schema.getSchema(),
            schema.getReferences(),
            resolveReferences
        );
    }
}
