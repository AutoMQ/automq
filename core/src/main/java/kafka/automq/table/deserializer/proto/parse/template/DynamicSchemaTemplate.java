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
import kafka.automq.table.deserializer.proto.parse.converter.ProtoElementSchemaConvert;
import kafka.automq.table.deserializer.proto.schema.DynamicSchema;

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants.JAVA_MULTIPLE_FILES_OPTION;
import static kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants.JAVA_OUTER_CLASSNAME_OPTION;
import static kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants.JAVA_PACKAGE_OPTION;


/**
 * Abstract template class that defines the skeleton algorithm for converting source objects into DynamicSchema.
 * This class implements the Template Method pattern, providing a structured approach to schema conversion
 * while allowing specific steps to be customized by subclasses.
 *
 * @param <T> The type of the source object to convert from
 */
public abstract class DynamicSchemaTemplate<T> {
    
    /**
     * Gets the converter implementation for the specific source type.
     *
     * @return The converter implementation
     */
    protected abstract DynamicSchemaConverter<T> getConverter();


    /**
     * Processes schema options and applies them to the schema builder.
     *
     * @param schema The schema builder
     * @param options The list of options to process
     */
    protected void processSchemaOptions(DynamicSchema.Builder schema, List<OptionElement> options) {
        findOption(JAVA_PACKAGE_OPTION, options)
            .ifPresent(o -> schema.setJavaPackage(o.getValue().toString()));

        findOption(JAVA_OUTER_CLASSNAME_OPTION, options)
            .ifPresent(o -> schema.setJavaOuterClassname(o.getValue().toString()));

        findOption(JAVA_MULTIPLE_FILES_OPTION, options)
            .ifPresent(o -> schema.setJavaMultipleFiles(Boolean.valueOf(o.getValue().toString())));
    }

    /**
     * Finds an option by name in a list of options.
     *
     * @param name The option name
     * @param options The list of options
     * @return Optional containing the found option, or empty if not found
     */
    public static Optional<OptionElement> findOption(String name, List<OptionElement> options) {
        return options.stream()
            .filter(o -> o.getName().equals(name))
            .findFirst();
    }

    /**
     * Converts the source object into a DynamicSchema.
     * This method implements the template method pattern, defining the skeleton of the conversion algorithm
     * while delegating specific conversion steps to the converter.
     *
     * @param name The name of the schema
     * @param source The source object to convert
     * @param dependencies Map of dependencies
     * @return The converted DynamicSchema
     * @throws Descriptors.DescriptorValidationException if validation fails
     */
    public DynamicSchema convert(String name, T source, Map<String, ProtoFileElement> dependencies) 
        throws Descriptors.DescriptorValidationException {
        
        DynamicSchemaConverter<T> converter = getConverter();
        ProtoElementSchemaConvert elementSchemaConvert = new ProtoElementSchemaConvert();
        DynamicSchema.Builder schema = DynamicSchema.newBuilder();

        // Set basic properties
        Optional.ofNullable(converter.getSyntax(source))
            .ifPresent(schema::setSyntax);

        Optional.ofNullable(converter.getPackageName(source))
            .ifPresent(schema::setPackage);

        // Process types
        for (TypeElement typeElem : converter.getTypes(source)) {
            if (typeElem instanceof MessageElement) {
                MessageElement messageElement = (MessageElement) typeElem;
                schema.addMessageDefinition(elementSchemaConvert.convert(messageElement));
            } else if (typeElem instanceof EnumElement) {
                EnumElement enumElement = (EnumElement) typeElem;
                schema.addEnumDefinition(elementSchemaConvert.convert(enumElement));
            }
        }

        // Process imports and dependencies
        processImportsAndDependencies(schema, source, converter, dependencies);

        // Process options
        processSchemaOptions(schema, converter.getOptions(source));

        schema.setName(name);
        return schema.build();
    }

    /**
     * Processes imports and dependencies, adding them to the schema builder.
     *
     * @param schema The schema builder
     * @param source The source object
     * @param converter The converter instance
     * @param dependencies Map of dependencies
     */
    protected void processImportsAndDependencies(DynamicSchema.Builder schema, T source,
                                               DynamicSchemaConverter<T> converter,
                                               Map<String, ProtoFileElement> dependencies) {
        // Process regular imports
        for (String ref : converter.getImports(source)) {
            ProtoFileElement dep = dependencies.get(ref) == null ? ProtoConstants.BASE_DEPENDENCIES.get(ref) : dependencies.get(ref);
            if (dep != null) {
                schema.addDependency(ref);
                try {
                    schema.addSchema(convert(ref, (T) dep, dependencies));
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalStateException("Failed to process import: " + ref, e);
                }
            }
        }

        // Process public imports
        for (String ref : converter.getPublicImports(source)) {
            ProtoFileElement dep = dependencies.get(ref) == null ? ProtoConstants.BASE_DEPENDENCIES.get(ref) : dependencies.get(ref);
            if (dep != null) {
                schema.addPublicDependency(ref);
                try {
                    schema.addSchema(convert(ref, (T) dep, dependencies));
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalStateException("Failed to process public import: " + ref, e);
                }
            }
        }
    }
} 