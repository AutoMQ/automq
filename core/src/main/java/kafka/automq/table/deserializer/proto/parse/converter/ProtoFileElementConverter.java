package kafka.automq.table.deserializer.proto.parse.converter;

import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import java.util.List;

/**
 * Implementation of DynamicSchemaConverter for ProtoFileElement source objects.
 * This class provides specific conversion logic for extracting schema information
 * from ProtoFileElement instances.
 */
public class ProtoFileElementConverter implements DynamicSchemaConverter<ProtoFileElement> {
    
    @Override
    public String getSyntax(ProtoFileElement source) {
        return source.getSyntax() != null ? source.getSyntax().toString() : null;
    }

    @Override
    public String getPackageName(ProtoFileElement source) {
        String packageName = source.getPackageName();
        return packageName != null ? packageName : "";
    }

    @Override
    public List<TypeElement> getTypes(ProtoFileElement source) {
        return source.getTypes();
    }

    @Override
    public List<String> getImports(ProtoFileElement source) {
        return source.getImports();
    }

    @Override
    public List<String> getPublicImports(ProtoFileElement source) {
        return source.getPublicImports();
    }

    @Override
    public List<OptionElement> getOptions(ProtoFileElement source) {
        return source.getOptions();
    }

    @Override
    public List<ServiceElement> getServices(ProtoFileElement source) {
        return source.getServices();
    }
} 