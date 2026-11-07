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

import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.List;

/**
 * Interface defining the contract for converting source objects into components needed for DynamicSchema creation.
 * This interface provides a standardized way to extract necessary information from different source types
 * that can be used to build a DynamicSchema.
 *
 * @param <T> The type of the source object to convert from (e.g., ProtoFileElement, FileDescriptorProto)
 */
public interface DynamicSchemaConverter<T> {
    
    /**
     * Extracts the syntax version from the source.
     *
     * @param source The source object
     * @return The syntax version as a string, or null if not specified
     */
    String getSyntax(T source);

    /**
     * Retrieves the package name from the source.
     *
     * @param source The source object
     * @return The package name, or null if not specified
     */
    String getPackageName(T source);

    /**
     * Gets all type definitions (messages and enums) from the source.
     *
     * @param source The source object
     * @return List of TypeElements representing messages and enums
     */
    List<TypeElement> getTypes(T source);

    /**
     * Retrieves the list of imports from the source.
     *
     * @param source The source object
     * @return List of import file paths
     */
    List<String> getImports(T source);

    /**
     * Retrieves the list of public imports from the source.
     *
     * @param source The source object
     * @return List of public import file paths
     */
    List<String> getPublicImports(T source);

    /**
     * Retrieves all options defined in the source.
     *
     * @param source The source object
     * @return List of OptionElements
     */
    List<OptionElement> getOptions(T source);

    /**
     * Retrieves all service definitions from the source.
     * @param source The source object
     * @return List of ServiceElements
     */
    List<ServiceElement> getServices(T source);

} 