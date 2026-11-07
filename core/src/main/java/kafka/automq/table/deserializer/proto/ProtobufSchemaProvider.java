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

import java.util.Map;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;

public class ProtobufSchemaProvider extends AbstractSchemaProvider {

    @Override
    public String schemaType() {
        return "PROTOBUF";
    }

    @Override
    public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
        Map<String, String> resolveReferences = resolveReferences(schema);
        return new CustomProtobufSchema(
            null,
            schema.getVersion(),
            schema.getMetadata(),
            schema.getRuleSet(),
            schema.getSchema(),
            schema.getReferences(),
            resolveReferences
        );
    }
}
