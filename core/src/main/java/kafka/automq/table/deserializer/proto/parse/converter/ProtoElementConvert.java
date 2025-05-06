package kafka.automq.table.deserializer.proto.parse.converter;

import kafka.automq.table.deserializer.proto.schema.EnumDefinition;
import kafka.automq.table.deserializer.proto.schema.MessageDefinition;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;

/**
 * Visitor interface for processing protobuf elements.
 * Implementations can provide different ways to process these elements.
 */
public interface ProtoElementConvert {
    EnumDefinition convert(EnumElement enumElement);
    MessageDefinition convert(MessageElement messageElement);
}
