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

package kafka.automq.table.deserializer.proto;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

public class CustomProtobufSchema implements ParsedSchema {
    private final String name;
    private final Integer version;
    private final Metadata metadata;
    private final RuleSet ruleSet;
    private final String schemaDefinition;
    private final List<SchemaReference> references;
    private final Map<String, String> resolvedReferences;

    public CustomProtobufSchema(
        String name, Integer version,
        Metadata metadata, RuleSet ruleSet,
        String schemaDefinition, List<SchemaReference> references, Map<String, String> resolvedReferences) {
        this.name = name;
        this.version = version;
        this.metadata = metadata;
        this.ruleSet = ruleSet;
        this.schemaDefinition = schemaDefinition;
        this.references = references;
        this.resolvedReferences = resolvedReferences;
    }

    @Override
    public String schemaType() {
        return "PROTOBUF";
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String canonicalString() {
        return schemaDefinition;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public List<SchemaReference> references() {
        return references;
    }

    public Map<String, String> resolvedReferences() {
        return resolvedReferences;
    }

    @Override
    public Metadata metadata() {
        return metadata;
    }

    @Override
    public RuleSet ruleSet() {
        return ruleSet;
    }

    @Override
    public ParsedSchema copy() {
        return new CustomProtobufSchema(name, version, metadata, ruleSet, schemaDefinition, references, resolvedReferences);
    }

    @Override
    public ParsedSchema copy(Integer version) {
        return new CustomProtobufSchema(name, version, metadata, ruleSet, schemaDefinition, references, resolvedReferences);
    }

    @Override
    public ParsedSchema copy(Metadata metadata, RuleSet ruleSet) {
        return new CustomProtobufSchema(name, version, metadata, ruleSet, schemaDefinition, references, resolvedReferences);
    }

    @Override
    public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd, Map<SchemaEntity, Set<String>> tagsToRemove) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Object rawSchema() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
