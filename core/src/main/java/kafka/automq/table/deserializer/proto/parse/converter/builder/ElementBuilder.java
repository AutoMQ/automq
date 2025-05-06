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
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.OptionElement;

/**
 * Abstract builder for converting protobuf elements to Wire Schema elements.
 */
public abstract class ElementBuilder<T, B extends ElementBuilder<T, B>> {
    protected static final Location DEFAULT_LOCATION = Location.get("");
    protected static final String DOCUMENTATION = "";
    protected final String name;
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