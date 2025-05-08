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
            DOCUMENTATION,
            fields.build(),
            Collections.emptyList(),
            Collections.emptyList(),
            DEFAULT_LOCATION
        );
    }
} 