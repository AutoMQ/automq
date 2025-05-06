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
import kafka.automq.table.deserializer.proto.parse.converter.ProtoFileElementConverter;

import com.squareup.wire.schema.internal.parser.ProtoFileElement;

/**
 * Template implementation for converting ProtoFileElement to DynamicSchema.
 * This class provides the concrete implementation of the template methods
 * specific to ProtoFileElement processing.
 */
public class ProtoFileElementTemplate extends DynamicSchemaTemplate<ProtoFileElement> {
    
    private final ProtoFileElementConverter converter;

    /**
     * Creates a new ProtoFileElementTemplate with a default converter.
     */
    public ProtoFileElementTemplate() {
        this.converter = new ProtoFileElementConverter();
    }

    @Override
    protected DynamicSchemaConverter<ProtoFileElement> getConverter() {
        return converter;
    }
} 