package kafka.automq.table.deserializer.proto.parse.converter.builder;

import com.google.common.collect.ImmutableList;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import java.util.Collections;

/**
 * Builder for OneOfElement construction.
 */
public class OneOfBuilder extends ElementBuilder<OneOfElement, OneOfBuilder> {
    private final ImmutableList.Builder<FieldElement> fields = ImmutableList.builder();

    public OneOfBuilder(String name) {
        super(name);
    }

    public OneOfBuilder addField(FieldElement field) {
        fields.add(field);
        return this;
    }

    public ImmutableList<FieldElement> getFields() {
        return fields.build();
    }

    @Override
    protected OneOfElement build() {
        return new OneOfElement(
            name,
            documentation,
            fields.build(),
            Collections.emptyList(),
            Collections.emptyList(),
            DEFAULT_LOCATION
        );
    }
} 