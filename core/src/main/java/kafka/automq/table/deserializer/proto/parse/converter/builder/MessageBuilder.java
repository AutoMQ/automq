/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            DOCUMENTATION,
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