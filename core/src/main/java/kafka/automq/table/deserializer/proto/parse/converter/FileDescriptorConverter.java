package kafka.automq.table.deserializer.proto.parse.converter;

import kafka.automq.table.deserializer.proto.parse.converter.builder.ElementBuilder;
import kafka.automq.table.deserializer.proto.parse.converter.builder.EnumBuilder;
import kafka.automq.table.deserializer.proto.parse.converter.builder.MessageBuilder;
import kafka.automq.table.deserializer.proto.parse.converter.builder.OneOfBuilder;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.RpcElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kotlin.ranges.IntRange;

import static kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants.DEFAULT_LOCATION;

/**
 * Converter implementation for FileDescriptor to Wire Schema elements.
 */
public class FileDescriptorConverter implements DynamicSchemaConverter<DescriptorProtos.FileDescriptorProto> {

    @Override
    public List<String> getImports(DescriptorProtos.FileDescriptorProto source) {
        List<String> imports = new ArrayList<>();
        List<String> dependencyList = source.getDependencyList();
        Set<Integer> publicDependencyList = new HashSet<>(source.getPublicDependencyList());

        for (int i = 0; i < dependencyList.size(); i++) {
            if (!publicDependencyList.contains(i)) {
                imports.add(dependencyList.get(i));
            }
        }
        return imports;
    }

    @Override
    public List<String> getPublicImports(DescriptorProtos.FileDescriptorProto source) {
        List<String> publicImports = new ArrayList<>();
        List<String> dependencyList = source.getDependencyList();
        List<Integer> publicDependencyList = source.getPublicDependencyList();

        for (Integer index : publicDependencyList) {
            publicImports.add(dependencyList.get(index));
        }
        return publicImports;
    }

    @Override
    public String getSyntax(DescriptorProtos.FileDescriptorProto source) {
        return source.getSyntax();
    }

    @Override
    public String getPackageName(DescriptorProtos.FileDescriptorProto source) {
        String aPackage = source.getPackage();
        return aPackage.isEmpty() ? null : aPackage;
    }

    @Override
    public List<TypeElement> getTypes(DescriptorProtos.FileDescriptorProto source) {
        List<TypeElement> types = new ArrayList<>();
        // Convert messages
        for (DescriptorProtos.DescriptorProto messageType : source.getMessageTypeList()) {
            types.add(convertMessage(messageType, source));
        }

        // Convert enums
        for (DescriptorProtos.EnumDescriptorProto enumType : source.getEnumTypeList()) {
            types.add(convertEnum(enumType));
        }

        return types;
    }

    @Override
    public List<ServiceElement> getServices(DescriptorProtos.FileDescriptorProto source) {
        List<ServiceElement> services = new ArrayList<>();
        for (DescriptorProtos.ServiceDescriptorProto service : source.getServiceList()) {
            services.add(convertService(service));
        }
        return services;
    }

    @Override
    public List<OptionElement> getOptions(DescriptorProtos.FileDescriptorProto source) {
        List<OptionElement> options = new ArrayList<>();
        DescriptorProtos.FileOptions fileOptions = source.getOptions();

        // Java related options
        addOptionIfPresent(options, ProtoConstants.JAVA_PACKAGE_OPTION, fileOptions.hasJavaPackage(), fileOptions.getJavaPackage());
        addOptionIfPresent(options, ProtoConstants.JAVA_OUTER_CLASSNAME_OPTION, fileOptions.hasJavaOuterClassname(), fileOptions.getJavaOuterClassname());
        addOptionIfPresent(options, ProtoConstants.JAVA_MULTIPLE_FILES_OPTION, fileOptions.hasJavaMultipleFiles(), fileOptions.getJavaMultipleFiles());
        addOptionIfPresent(options, ProtoConstants.JAVA_GENERATE_EQUALS_AND_HASH_OPTION, fileOptions.hasJavaGenerateEqualsAndHash(), fileOptions.getJavaGenerateEqualsAndHash());
        addOptionIfPresent(options, ProtoConstants.JAVA_GENERIC_SERVICES_OPTION, fileOptions.hasJavaGenericServices(), fileOptions.getJavaGenericServices());
        addOptionIfPresent(options, ProtoConstants.JAVA_STRING_CHECK_UTF8_OPTION, fileOptions.hasJavaStringCheckUtf8(), fileOptions.getJavaStringCheckUtf8());

        // C++ related options
        addOptionIfPresent(options, ProtoConstants.CC_GENERIC_SERVICES_OPTION, fileOptions.hasCcGenericServices(), fileOptions.getCcGenericServices());
        addOptionIfPresent(options, ProtoConstants.CC_ENABLE_ARENAS_OPTION, fileOptions.hasCcEnableArenas(), fileOptions.getCcEnableArenas());

        // C# related options
        addOptionIfPresent(options, ProtoConstants.CSHARP_NAMESPACE_OPTION, fileOptions.hasCsharpNamespace(), fileOptions.getCsharpNamespace());

        // Go related options
        addOptionIfPresent(options, ProtoConstants.GO_PACKAGE_OPTION, fileOptions.hasGoPackage(), fileOptions.getGoPackage());

        // Objective-C related options
        addOptionIfPresent(options, ProtoConstants.OBJC_CLASS_PREFIX_OPTION, fileOptions.hasObjcClassPrefix(), fileOptions.getObjcClassPrefix());

        // PHP related options
        addOptionIfPresent(options, ProtoConstants.PHP_CLASS_PREFIX_OPTION, fileOptions.hasPhpClassPrefix(), fileOptions.getPhpClassPrefix());
//        addOptionIfPresent(options, ProtoConstants.PHP_GENERIC_SERVICES_OPTION, fileOptions.hasPhpGenericServices(), fileOptions.getPhpGenericServices());
        addOptionIfPresent(options, ProtoConstants.PHP_METADATA_NAMESPACE_OPTION, fileOptions.hasPhpMetadataNamespace(), fileOptions.getPhpMetadataNamespace());
        addOptionIfPresent(options, ProtoConstants.PHP_NAMESPACE_OPTION, fileOptions.hasPhpNamespace(), fileOptions.getPhpNamespace());

        // Python related options
        addOptionIfPresent(options, ProtoConstants.PY_GENERIC_SERVICES_OPTION, fileOptions.hasPyGenericServices(), fileOptions.getPyGenericServices());

        // Ruby related options
        addOptionIfPresent(options, ProtoConstants.RUBY_PACKAGE_OPTION, fileOptions.hasRubyPackage(), fileOptions.getRubyPackage());

        // Swift related options
        addOptionIfPresent(options, ProtoConstants.SWIFT_PREFIX_OPTION, fileOptions.hasSwiftPrefix(), fileOptions.getSwiftPrefix());

        // Optimize related options
        addOptionIfPresent(options, ProtoConstants.OPTIMIZE_FOR_OPTION, fileOptions.hasOptimizeFor(), fileOptions.getOptimizeFor());

        return options;
    }


    private MessageElement convertMessage(DescriptorProtos.DescriptorProto descriptor, DescriptorProtos.FileDescriptorProto file) {
        MessageBuilder builder = new MessageBuilder(descriptor.getName());
        
        // Add options
        new MessageOptionStrategy().addOption(builder, descriptor.getOptions());

        // Process fields and oneofs
        Map<Integer, OneOfBuilder> oneofBuilders = new HashMap<>();
        for (DescriptorProtos.OneofDescriptorProto oneof : descriptor.getOneofDeclList()) {
            oneofBuilders.put(oneofBuilders.size(), builder.newOneOf(oneof.getName()));
        }

        // Process fields
        for (DescriptorProtos.FieldDescriptorProto field : descriptor.getFieldList()) {
            FieldElement fieldElement = convertField(file, field, field.hasOneofIndex());
            if (field.hasOneofIndex()) {
                oneofBuilders.get(field.getOneofIndex()).addField(fieldElement);
            } else {
                builder.addField(fieldElement);
            }
        }

        // Process nested types
        for (DescriptorProtos.DescriptorProto nestedType : descriptor.getNestedTypeList()) {
            builder.addNestedType(convertMessage(nestedType, file));
        }
        for (DescriptorProtos.EnumDescriptorProto enumType : descriptor.getEnumTypeList()) {
            builder.addNestedType(convertEnum(enumType));
        }

        // Process reserved ranges and names
        processReservedRanges(descriptor, builder);
        processExtensionRanges(descriptor, builder);

        return builder.build();
    }

    private void processReservedRanges(DescriptorProtos.DescriptorProto descriptor, MessageBuilder builder) {
        for (String reservedName : descriptor.getReservedNameList()) {
            builder.addReserved(new ReservedElement(DEFAULT_LOCATION, "", Collections.singletonList(reservedName)));
        }
        
        for (DescriptorProtos.DescriptorProto.ReservedRange range : descriptor.getReservedRangeList()) {
            List<Object> values = new ArrayList<>();
            values.add(new IntRange(range.getStart(), range.getEnd() - 1));
            builder.addReserved(new ReservedElement(DEFAULT_LOCATION, "", values));
        }
    }

    private void processExtensionRanges(DescriptorProtos.DescriptorProto descriptor, MessageBuilder builder) {
        for (DescriptorProtos.DescriptorProto.ExtensionRange range : descriptor.getExtensionRangeList()) {
            List<Object> values = new ArrayList<>();
            values.add(new IntRange(range.getStart(), range.getEnd() - 1));
            builder.addExtension(new ExtensionsElement(DEFAULT_LOCATION, "", values));
        }
    }

    private EnumElement convertEnum(DescriptorProtos.EnumDescriptorProto enumType) {
        EnumBuilder builder = new EnumBuilder(enumType.getName());
        
        // Add options
        new EnumOptionStrategy().addOption(builder, enumType.getOptions());

        // Add constants
        for (DescriptorProtos.EnumValueDescriptorProto value : enumType.getValueList()) {
            builder.addConstant(value.getName(), value.getNumber());
        }

        // Process reserved ranges and names
        for (DescriptorProtos.EnumDescriptorProto.EnumReservedRange range : enumType.getReservedRangeList()) {
            List<Object> values = new ArrayList<>();
            values.add(new IntRange(range.getStart(), range.getEnd() - 1));
            builder.addReserved(new ReservedElement(DEFAULT_LOCATION, "", values));
        }

        for (String reservedName : enumType.getReservedNameList()) {
            builder.addReserved(new ReservedElement(DEFAULT_LOCATION, "", Collections.singletonList(reservedName)));
        }

        return builder.build();
    }

    private ServiceElement convertService(DescriptorProtos.ServiceDescriptorProto sv) {
        String name = sv.getName();
        ImmutableList.Builder<RpcElement> rpcs = ImmutableList.builder();
        
        // Convert methods
        for (DescriptorProtos.MethodDescriptorProto md : sv.getMethodList()) {
            // Build method options
            ImmutableList.Builder<OptionElement> methodOptions = ImmutableList.builder();
            
            if (md.getOptions().hasDeprecated()) {
                methodOptions.add(new OptionElement(ProtoConstants.DEPRECATED_OPTION, OptionElement.Kind.BOOLEAN,
                    md.getOptions().getDeprecated(), false));
            }
            
            if (md.getOptions().hasIdempotencyLevel()) {
                methodOptions.add(new OptionElement(ProtoConstants.IDEMPOTENCY_LEVEL_OPTION, OptionElement.Kind.ENUM,
                    md.getOptions().getIdempotencyLevel(), false));
            }

            // Create RPC element
            rpcs.add(new RpcElement(DEFAULT_LOCATION, md.getName(), "", 
                getTypeName(md.getInputType()), 
                getTypeName(md.getOutputType()),
                md.getClientStreaming(), 
                md.getServerStreaming(), 
                methodOptions.build()));
        }

        // Build service options
        ImmutableList.Builder<OptionElement> serviceOptions = ImmutableList.builder();
        if (sv.getOptions().hasDeprecated()) {
            serviceOptions.add(new OptionElement(ProtoConstants.DEPRECATED_OPTION, OptionElement.Kind.BOOLEAN,
                sv.getOptions().getDeprecated(), false));
        }

        return new ServiceElement(DEFAULT_LOCATION, name, "", rpcs.build(), serviceOptions.build());
    }

    /**
     * Determines the field label based on the proto syntax version and field properties.
     */
    private Field.Label label(DescriptorProtos.FileDescriptorProto file, DescriptorProtos.FieldDescriptorProto fd) {
        boolean isProto3 = file.getSyntax().equals(ProtoConstants.PROTO3);
        switch (fd.getLabel()) {
            case LABEL_REQUIRED:
                return isProto3 ? null : Field.Label.REQUIRED;
            case LABEL_OPTIONAL:
                // If it's a Proto3 optional, we have to print the optional label.
                return isProto3 && !fd.hasProto3Optional() ? null : Field.Label.OPTIONAL;
            case LABEL_REPEATED:
                return Field.Label.REPEATED;
            default:
                throw new IllegalArgumentException("Unsupported label");
        }
    }

    /**
     * Returns the field type name, either from the type name or the primitive type.
     */
    private String dataType(DescriptorProtos.FieldDescriptorProto field) {
        if (field.hasTypeName()) {
            return field.getTypeName();
        } else {
            DescriptorProtos.FieldDescriptorProto.Type type = field.getType();
            return Descriptors.FieldDescriptor.Type.valueOf(type).name().toLowerCase();
        }
    }


    private FieldElement convertField(DescriptorProtos.FileDescriptorProto file, DescriptorProtos.FieldDescriptorProto fd, boolean inOneof) {
        String name = fd.getName();
        DescriptorProtos.FieldOptions fieldDescriptorOptions = fd.getOptions();
        List<OptionElement> optionElements = new ArrayList<>();
        // Add standard field options if present
        if (fd.hasJsonName() && !fd.getJsonName().equals(getDefaultJsonName(name))) {
            optionElements.add(new OptionElement(ProtoConstants.JSON_NAME_OPTION, OptionElement.Kind.STRING,
                fd.getJsonName(), false));
        }
        addOptionIfPresent(optionElements, ProtoConstants.PACKED_OPTION, fieldDescriptorOptions.hasPacked(), fd.getOptions().getPacked());
        addOptionIfPresent(optionElements, ProtoConstants.DEPRECATED_OPTION, fieldDescriptorOptions.hasDeprecated(), fieldDescriptorOptions.getDeprecated());
        addOptionIfPresent(optionElements, ProtoConstants.CTYPE_OPTION, fieldDescriptorOptions.hasCtype(), fieldDescriptorOptions.getCtype());
        addOptionIfPresent(optionElements, ProtoConstants.JSTYPE_OPTION, fieldDescriptorOptions.hasJstype(), fieldDescriptorOptions.getJstype());
        ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
        options.addAll(optionElements);

        String jsonName = null; // Let Wire calculate the default JSON name
        String defaultValue = fd.hasDefaultValue() && fd.getDefaultValue() != null ? fd.getDefaultValue() : null;

        return new FieldElement(
            DEFAULT_LOCATION,
            inOneof ? null : label(file, fd),
            dataType(fd),
            name,
            defaultValue,
            jsonName,
            fd.getNumber(),
            "",
            options.build()
        );
    }


    /**
     * Strategy interface for handling different types of options.
     */
    private interface OptionStrategy {
        void addOption(ElementBuilder<?, ?> builder, Object options);
    }

    /**
     * Strategy for handling message options.
     */
    private static class MessageOptionStrategy implements OptionStrategy {
        @Override
        public void addOption(ElementBuilder<?, ?> builder, Object options) {
            DescriptorProtos.MessageOptions msgOptions = (DescriptorProtos.MessageOptions) options;
            if (msgOptions.hasMapEntry()) {
                builder.addOption(ProtoConstants.MAP_ENTRY_OPTION, OptionElement.Kind.BOOLEAN, msgOptions.getMapEntry());
            }
            if (msgOptions.hasNoStandardDescriptorAccessor()) {
                builder.addOption(ProtoConstants.NO_STANDARD_DESCRIPTOR_OPTION, OptionElement.Kind.BOOLEAN,
                                  msgOptions.getNoStandardDescriptorAccessor());
            }
        }
    }

    /**
     * Strategy for handling enum options.
     */
    private static class EnumOptionStrategy implements OptionStrategy {
        @Override
        public void addOption(ElementBuilder<?, ?> builder, Object options) {
            DescriptorProtos.EnumOptions enumOptions = (DescriptorProtos.EnumOptions) options;
            if (enumOptions.hasAllowAlias()) {
                builder.addOption(ProtoConstants.ALLOW_ALIAS_OPTION, OptionElement.Kind.BOOLEAN, enumOptions.getAllowAlias());
            }
        }
    }

    /**
     * Adds an option to the options list if it is present in the source.
     * Determines the appropriate OptionElement.Kind based on the value type.
     *
     * @param options The list of options to add to
     * @param name The name of the option
     * @param hasOption Whether the option is present
     * @param value The value of the option
     */
    private void addOptionIfPresent(List<OptionElement> options, String name, boolean hasOption, Object value) {
        if (hasOption) {
            OptionElement.Kind kind;
            if (value instanceof Boolean) {
                kind = OptionElement.Kind.BOOLEAN;
            } else if (value instanceof String) {
                kind = OptionElement.Kind.STRING;
            } else if (value instanceof Enum) {
                kind = OptionElement.Kind.ENUM;
            } else {
                kind = OptionElement.Kind.STRING;
            }
            options.add(new OptionElement(name, kind, value.toString(), false));
        }
    }


    private String getTypeName(String typeName) {
        return typeName.startsWith(".") ? typeName : "." + typeName;
    }

    /**
     * Calculates the default JSON name for a field following the protobuf convention.
     * Converts from snake_case to lowerCamelCase.
     */
    private String getDefaultJsonName(String fieldName) {
        String[] parts = fieldName.split("_");
        StringBuilder defaultJsonName = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; ++i) {
            defaultJsonName.append(parts[i].substring(0, 1).toUpperCase())
                .append(parts[i].substring(1));
        }
        return defaultJsonName.toString();
    }
}
