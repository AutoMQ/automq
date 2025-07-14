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

package kafka.automq.table.deserializer.proto.parse.template;

import kafka.automq.table.deserializer.proto.parse.converter.DynamicSchemaConverter;
import kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants;

import com.google.common.collect.ImmutableList;
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.Collections;

/**
 * Template method for converting protobuf schema representations
 */
public abstract class ProtoSchemaTemplate<T> {

    protected abstract DynamicSchemaConverter<T> getConverter();

    public ProtoFileElement convert(T source) {
        DynamicSchemaConverter<T> converter = getConverter();

        Syntax syntax = ProtoConstants.PROTO3.equals(converter.getSyntax(source)) ?
            Syntax.PROTO_3 : Syntax.PROTO_2;

        String packageName = converter.getPackageName(source);

        ImmutableList.Builder<String> imports = ImmutableList.builder();
        imports.addAll(converter.getImports(source));

        ImmutableList.Builder<String> publicImports = ImmutableList.builder();
        publicImports.addAll(converter.getPublicImports(source));

        ImmutableList.Builder<TypeElement> types = ImmutableList.builder();
        types.addAll(converter.getTypes(source));

        ImmutableList.Builder<ServiceElement> services = ImmutableList.builder();
        services.addAll(converter.getServices(source));

        ImmutableList.Builder<OptionElement> options = ImmutableList.builder();
        options.addAll(converter.getOptions(source));

        return new ProtoFileElement(
            ProtoConstants.DEFAULT_LOCATION,
            packageName,
            syntax,
            imports.build(),
            publicImports.build(),
            types.build(),
            services.build(),
            Collections.emptyList(), // extends
            options.build()
        );
    }
}
