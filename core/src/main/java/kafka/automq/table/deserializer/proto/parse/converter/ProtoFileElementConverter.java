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

package kafka.automq.table.deserializer.proto.parse.converter;

import com.squareup.wire.Syntax;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.List;

/**
 * Implementation of DynamicSchemaConverter for ProtoFileElement source objects.
 * This class provides specific conversion logic for extracting schema information
 * from ProtoFileElement instances.
 */
public class ProtoFileElementConverter implements DynamicSchemaConverter<ProtoFileElement> {
    
    @Override
    public String getSyntax(ProtoFileElement source) {
        Syntax syntax = source.getSyntax();
        return syntax != null ? syntax.toString() : null;
    }

    @Override
    public String getPackageName(ProtoFileElement source) {
        String packageName = source.getPackageName();
        return packageName != null ? packageName : "";
    }

    @Override
    public List<TypeElement> getTypes(ProtoFileElement source) {
        return source.getTypes();
    }

    @Override
    public List<String> getImports(ProtoFileElement source) {
        return source.getImports();
    }

    @Override
    public List<String> getPublicImports(ProtoFileElement source) {
        return source.getPublicImports();
    }

    @Override
    public List<OptionElement> getOptions(ProtoFileElement source) {
        return source.getOptions();
    }

    @Override
    public List<ServiceElement> getServices(ProtoFileElement source) {
        return source.getServices();
    }
} 