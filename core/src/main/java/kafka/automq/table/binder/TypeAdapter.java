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
package kafka.automq.table.binder;

import org.apache.iceberg.types.Type;

/**
 * Converts values between different schema systems.
 *
 * @param <S> The source schema type (e.g., org.apache.avro.Schema)
 */
public interface TypeAdapter<S> {

    /**
     * Converts a source value to the target Iceberg type.
     *
     * @param sourceValue  The source value
     * @param sourceSchema The source schema
     * @param targetType   The target Iceberg type
     * @return The converted value
     */
    Object convert(Object sourceValue, S sourceSchema, Type targetType);

    /**
     * Converts a source value to the target Iceberg type with support for recursive struct conversion.
     *
     * @param sourceValue     The source value
     * @param sourceSchema    The source schema
     * @param targetType      The target Iceberg type
     * @param structConverter A callback for converting nested STRUCT types
     * @return The converted value
     */
    Object convert(Object sourceValue, S sourceSchema, Type targetType, StructConverter<S> structConverter);
}
