package kafka.automq.table.deserializer.proto.parse.converter;

import com.google.protobuf.AnyProto;
import com.google.protobuf.ApiProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DurationProto;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.SourceContextProto;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.TypeProto;
import com.google.protobuf.WrappersProto;
import com.google.type.CalendarPeriodProto;
import com.google.type.ColorProto;
import com.google.type.DateProto;
import com.google.type.DayOfWeek;
import com.google.type.ExprProto;
import com.google.type.FractionProto;
import com.google.type.IntervalProto;
import com.google.type.LatLng;
import com.google.type.LocalizedTextProto;
import com.google.type.MoneyProto;
import com.google.type.MonthProto;
import com.google.type.PhoneNumberProto;
import com.google.type.PostalAddressProto;
import com.google.type.QuaternionProto;
import com.google.type.TimeOfDayProto;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static kafka.automq.table.deserializer.proto.parse.ProtobufSchemaParser.toProtoFileElement;

/**
 * Constants used in protobuf schema parsing and conversion.
 */
public final class ProtoConstants {
    /** Default location for proto file elements */
    public static final Location DEFAULT_LOCATION = Location.get("");

    /** Protocol buffer syntax versions */
    public static final String PROTO2 = "proto2";
    public static final String PROTO3 = "proto3";

    /** Common protobuf option names */
    public static final String ALLOW_ALIAS_OPTION = "allow_alias";
    public static final String MAP_ENTRY_OPTION = "map_entry";
    public static final String DEPRECATED_OPTION = "deprecated";
    public static final String PACKED_OPTION = "packed";
    public static final String JSON_NAME_OPTION = "json_name";
    public static final String CTYPE_OPTION = "ctype";
    public static final String JSTYPE_OPTION = "jstype";

    /** Map field related constants */
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String MAP_ENTRY_SUFFIX = "Entry";

    /** Message options */
    public static final String NO_STANDARD_DESCRIPTOR_OPTION = "no_standard_descriptor_accessor";

    /** RPC options */
    public static final String IDEMPOTENCY_LEVEL_OPTION = "idempotency_level";

    /** Java related options */
    public static final String JAVA_PACKAGE_OPTION = "java_package";
    public static final String JAVA_OUTER_CLASSNAME_OPTION = "java_outer_classname";
    public static final String JAVA_MULTIPLE_FILES_OPTION = "java_multiple_files";
    public static final String JAVA_GENERATE_EQUALS_AND_HASH_OPTION = "java_generate_equals_and_hash";
    public static final String JAVA_GENERIC_SERVICES_OPTION = "java_generic_services";
    public static final String JAVA_STRING_CHECK_UTF8_OPTION = "java_string_check_utf8";

    /** C++ related options */
    public static final String CC_GENERIC_SERVICES_OPTION = "cc_generic_services";
    public static final String CC_ENABLE_ARENAS_OPTION = "cc_enable_arenas";

    /** C# related options */
    public static final String CSHARP_NAMESPACE_OPTION = "csharp_namespace";

    /** Go related options */
    public static final String GO_PACKAGE_OPTION = "go_package";

    /** Objective-C related options */
    public static final String OBJC_CLASS_PREFIX_OPTION = "objc_class_prefix";

    /** PHP related options */
    public static final String PHP_CLASS_PREFIX_OPTION = "php_class_prefix";
    public static final String PHP_GENERIC_SERVICES_OPTION = "php_generic_services";
    public static final String PHP_METADATA_NAMESPACE_OPTION = "php_metadata_namespace";
    public static final String PHP_NAMESPACE_OPTION = "php_namespace";

    /** Python related options */
    public static final String PY_GENERIC_SERVICES_OPTION = "py_generic_services";

    /** Ruby related options */
    public static final String RUBY_PACKAGE_OPTION = "ruby_package";

    /** Swift related options */
    public static final String SWIFT_PREFIX_OPTION = "swift_prefix";

    /** Optimize related options */
    public static final String OPTIMIZE_FOR_OPTION = "optimize_for";


    /** Well-known protobuf type descriptors */
    public static final Descriptors.FileDescriptor[] WELL_KNOWN_DEPENDENCIES;

    public static final Map<String, ProtoFileElement> BASE_DEPENDENCIES;

    static {
        // Support all the Protobuf WellKnownTypes
        // and the protos from Google API, https://github.com/googleapis/googleapis
        WELL_KNOWN_DEPENDENCIES = new Descriptors.FileDescriptor[] {
            ApiProto.getDescriptor().getFile(),
            FieldMaskProto.getDescriptor().getFile(),
            SourceContextProto.getDescriptor().getFile(),
            StructProto.getDescriptor().getFile(),
            TypeProto.getDescriptor().getFile(),
            TimestampProto.getDescriptor().getFile(),
            WrappersProto.getDescriptor().getFile(),
            AnyProto.getDescriptor().getFile(),
            EmptyProto.getDescriptor().getFile(),
            DurationProto.getDescriptor().getFile(),
            TimeOfDayProto.getDescriptor().getFile(),
            DateProto.getDescriptor().getFile(),
            CalendarPeriodProto.getDescriptor().getFile(),
            ColorProto.getDescriptor().getFile(),
            DayOfWeek.getDescriptor().getFile(),
            LatLng.getDescriptor().getFile(),
            FractionProto.getDescriptor().getFile(),
            MoneyProto.getDescriptor().getFile(),
            MonthProto.getDescriptor().getFile(),
            PhoneNumberProto.getDescriptor().getFile(),
            PostalAddressProto.getDescriptor().getFile(),
            LocalizedTextProto.getDescriptor().getFile(),
            IntervalProto.getDescriptor().getFile(),
            ExprProto.getDescriptor().getFile(),
            QuaternionProto.getDescriptor().getFile(),
        };

        BASE_DEPENDENCIES = Arrays.stream(WELL_KNOWN_DEPENDENCIES)
            .collect(Collectors.toMap(
                Descriptors.FileDescriptor::getFullName,
                item -> toProtoFileElement(item.toProto())));
    }
} 