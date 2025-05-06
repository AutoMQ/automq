package kafka.automq.table.deserializer.proto.parse.converter.builder;

import com.google.common.collect.ImmutableList;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;

/**
 * Builder for EnumElement construction.
 */
public class EnumBuilder extends ElementBuilder<EnumElement, EnumBuilder> {
    private final ImmutableList.Builder<EnumConstantElement> constants = ImmutableList.builder();
    private final ImmutableList.Builder<ReservedElement> reserved = ImmutableList.builder();

    public EnumBuilder(String name) {
        super(name);
    }

    public EnumBuilder addConstant(String name, int number) {
        constants.add(new EnumConstantElement(
            DEFAULT_LOCATION,
            name,
            number,
            documentation,
            ImmutableList.of()
        ));
        return this;
    }

    public EnumBuilder addReserved(ReservedElement element) {
        reserved.add(element);
        return this;
    }

    @Override
    public EnumElement build() {
        return new EnumElement(
            DEFAULT_LOCATION,
            name,
            documentation,
            options.build(),
            constants.build(),
            reserved.build()
        );
    }
} 