package kafka.automq.table.deserializer.proto.parse.converter.builder;

import com.google.common.collect.ImmutableList;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder for MessageElement construction.
 */
public class MessageBuilder extends ElementBuilder<MessageElement, MessageBuilder> {
    private final ImmutableList.Builder<FieldElement> fields = ImmutableList.builder();
    private final ImmutableList.Builder<TypeElement> nested = ImmutableList.builder();
    private final ImmutableList.Builder<ReservedElement> reserved = ImmutableList.builder();
    private final ImmutableList.Builder<ExtensionsElement> extensions = ImmutableList.builder();
    private final List<OneOfBuilder> oneofs = new ArrayList<>();

    public MessageBuilder(String name) {
        super(name);
    }

    public MessageBuilder addField(FieldElement field) {
        fields.add(field);
        return this;
    }

    public MessageBuilder addNestedType(TypeElement type) {
        nested.add(type);
        return this;
    }

    public MessageBuilder addReserved(ReservedElement element) {
        reserved.add(element);
        return this;
    }

    public MessageBuilder addExtension(ExtensionsElement element) {
        extensions.add(element);
        return this;
    }

    public OneOfBuilder newOneOf(String name) {
        OneOfBuilder builder = new OneOfBuilder(name);
        oneofs.add(builder);
        return builder;
    }

    @Override
    public MessageElement build() {
        return new MessageElement(
            DEFAULT_LOCATION,
            name,
            documentation,
            nested.build(),
            options.build(),
            reserved.build(),
            fields.build(),
            oneofs.stream()
                .filter(b -> !b.getFields().isEmpty())
                .map(OneOfBuilder::build)
                .collect(Collectors.toList()),
            extensions.build(),
            Collections.emptyList(),
            Collections.emptyList()
        );
    }
} 