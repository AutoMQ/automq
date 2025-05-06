package kafka.automq.table.deserializer.proto.parse.converter.builder;

import com.google.common.collect.ImmutableList;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.OptionElement;

/**
 * Abstract builder for converting protobuf elements to Wire Schema elements.
 */
public abstract class ElementBuilder<T, B extends ElementBuilder<T, B>> {
    protected static final Location DEFAULT_LOCATION = Location.get("");
    protected final String name;
    protected final String documentation = "";
    protected final ImmutableList.Builder<OptionElement> options = ImmutableList.builder();

    protected ElementBuilder(String name) {
        this.name = name;
    }

    protected abstract T build();

    @SuppressWarnings("unchecked")
    protected B self() {
        return (B) this;
    }

    public B addOption(String name, OptionElement.Kind kind, Object value) {
        options.add(new OptionElement(name, kind, value.toString(), false));
        return self();
    }
} 