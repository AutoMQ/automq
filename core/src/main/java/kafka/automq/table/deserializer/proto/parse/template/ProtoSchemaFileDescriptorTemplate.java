package kafka.automq.table.deserializer.proto.parse.template;

import kafka.automq.table.deserializer.proto.parse.converter.DynamicSchemaConverter;
import kafka.automq.table.deserializer.proto.parse.converter.FileDescriptorConverter;
import com.google.protobuf.DescriptorProtos;

public class ProtoSchemaFileDescriptorTemplate extends ProtoSchemaTemplate<DescriptorProtos.FileDescriptorProto> {
    @Override
    protected DynamicSchemaConverter<DescriptorProtos.FileDescriptorProto> getConverter() {
        return new FileDescriptorConverter();
    }
}
