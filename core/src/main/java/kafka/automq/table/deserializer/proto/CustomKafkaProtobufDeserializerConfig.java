package kafka.automq.table.deserializer.proto;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class CustomKafkaProtobufDeserializerConfig extends AbstractKafkaSchemaSerDeConfig {
    private static final ConfigDef CONFIG_DEF = baseConfigDef();
    public CustomKafkaProtobufDeserializerConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
    }
}
