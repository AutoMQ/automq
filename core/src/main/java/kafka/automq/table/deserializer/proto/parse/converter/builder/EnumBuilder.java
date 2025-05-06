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
            DOCUMENTATION,
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
            DOCUMENTATION,
            options.build(),
            constants.build(),
            reserved.build()
        );
    }
} 