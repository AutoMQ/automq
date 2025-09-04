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

import kafka.automq.table.process.convert.ConverterFactory;
import kafka.automq.table.process.convert.StringConverter;
import kafka.automq.table.process.transform.DebeziumUnwrapTransform;
import kafka.automq.table.process.transform.FlattenTransform;
import kafka.automq.table.process.transform.SchemalessTransform;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.server.record.TableTopicSchemaType;
import org.apache.kafka.server.record.TableTopicTransformType;

import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class RecordProcessorFactory {
    private final ConverterFactory converterFactory;

    public RecordProcessorFactory(String registryUrl) {
        this.converterFactory = new ConverterFactory(registryUrl);
    }

    public RecordProcessorFactory(String registryUrl, SchemaRegistryClient client) {
        this.converterFactory = new ConverterFactory(registryUrl, client);
    }

    public RecordProcessor create(WorkerConfig config, String topic) {
        // Handle deprecated configurations
        if (config.schemaType() == TableTopicSchemaType.SCHEMALESS) {
            return new DefaultRecordProcessor(topic, StringConverter.INSTANCE, StringConverter.INSTANCE, List.of(new SchemalessTransform()));
        }
        if (config.schemaType() == TableTopicSchemaType.SCHEMA) {
            return new DefaultRecordProcessor(topic,
                StringConverter.INSTANCE,
                converterFactory.createValueConverter(topic, config), List.of(FlattenTransform.INSTANCE));
        }

        var keyConverter = converterFactory.createKeyConverter(topic, config);
        var valueConverter = converterFactory.createValueConverter(topic, config);

        var transforms = createTransforms(config.transformType());

        return new DefaultRecordProcessor(topic, keyConverter, valueConverter, transforms);
    }

    private List<Transform> createTransforms(TableTopicTransformType transformType) {
        if (transformType == null || TableTopicTransformType.NONE.equals(transformType)) {
            return List.of();
        }
        switch (transformType) {
            case FLATTEN:
                return List.of(FlattenTransform.INSTANCE);
            case FLATTEN_DEBEZIUM:
                return List.of(FlattenTransform.INSTANCE, DebeziumUnwrapTransform.INSTANCE);
            default:
                throw new IllegalArgumentException("Unsupported transform type: " + transformType);
        }
    }
}
