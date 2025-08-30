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

package kafka.automq.table.process;

import kafka.automq.table.process.convert.RawConverter;
import kafka.automq.table.process.convert.RegistryConverterFactory;
import kafka.automq.table.process.transform.DebeziumUnwrapTransform;
import kafka.automq.table.process.transform.KafkaMetadataTransform;
import kafka.automq.table.process.transform.ValueUnwrapTransform;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.server.record.TableTopicSchemaType;
import org.apache.kafka.server.record.TableTopicTransformType;

import java.util.ArrayList;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class RecordProcessorFactory {
    private final RegistryConverterFactory registryConverterFactory;

    public RecordProcessorFactory(String registryUrl) {
        this.registryConverterFactory = new RegistryConverterFactory(registryUrl);
    }

    public RecordProcessorFactory(String registryUrl, SchemaRegistryClient client) {
        this.registryConverterFactory = new RegistryConverterFactory(registryUrl, client);
    }

    public RecordProcessor create(WorkerConfig config, String topic) {
        // Handle deprecated configurations
        if (config.schemaType() == TableTopicSchemaType.SCHEMALESS) {
            return new DefaultRecordProcessor(topic, new RawConverter());
        }
        if (config.schemaType() == TableTopicSchemaType.SCHEMA) {
            return new DefaultRecordProcessor(topic, registryConverterFactory.createForSchemaId(topic), List.of(new ValueUnwrapTransform()));
        }

        // Create converter based on convert type
        var converter = createConverter(config, topic);

        // Create transform list based on transform types
        var transforms = createTransforms(config.transformTypes());

        return new DefaultRecordProcessor(topic, converter, transforms);
    }

    private Converter createConverter(WorkerConfig config, String topic) {
        switch (config.convertType()) {
            case RAW:
                return new RawConverter();
            case BY_SCHEMA_ID:
                return registryConverterFactory.createForSchemaId(topic);
            case BY_SUBJECT_NAME:
                return registryConverterFactory.createForSubjectName(topic, config.convertBySubjectNameSubject(), config.convertBySubjectNameMessageFullName());
            default:
                throw new IllegalArgumentException("Unsupported convert type: " + config.convertType());
        }
    }

    private List<Transform> createTransforms(List<TableTopicTransformType> transformTypes) {
        if (transformTypes == null || transformTypes.isEmpty()) {
            return List.of();
        }
        List<Transform> transforms = new ArrayList<>();
        for (TableTopicTransformType type : transformTypes) {
            switch (type) {
                case VALUE_UNWRAP:
                    transforms.add(new ValueUnwrapTransform());
                    break;
                case DEBEZIUM_UNWRAP:
                    transforms.add(new DebeziumUnwrapTransform());
                    break;
                case KAFKA_METADATA:
                    transforms.add(new KafkaMetadataTransform());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported transform type: " + type);
            }
        }
        return transforms;
    }
}
