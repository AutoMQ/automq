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

package kafka.automq.table.transformer;

import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;

import org.apache.kafka.server.record.TableTopicSchemaType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import static kafka.automq.table.transformer.SchemaFormat.AVRO;

public class ConverterFactory {
    private final String registryUrl;
    private final Map<SchemaFormat, KafkaRecordConvert<GenericRecord>> recordConvertMap = new ConcurrentHashMap<>();
    private SchemaRegistryClient schemaRegistry;

    private final Cache<String, SchemaFormat> topicSchemaFormatCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(20))
        .maximumSize(10000)
        .build();

    private SchemaFormat getSchemaFormat(String topic) throws RestClientException, IOException {
        String subjectName = getSubjectName(topic);
        if (schemaRegistry != null) {
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subjectName);
            return SchemaFormat.fromString(schemaMetadata.getSchemaType());
        }
        return null;
    }

    public ConverterFactory(String registryUrl) {
        this.registryUrl = registryUrl;
        if (registryUrl != null) {
            schemaRegistry = new CachedSchemaRegistryClient(
                registryUrl,
                AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
                List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                null
            );
        }
    }

    public Converter converter(TableTopicSchemaType type, String topic) {
        switch (type) {
            case SCHEMALESS: {
                return new SchemalessConverter();
            }
            case SCHEMA: {
                return new LazyRegistrySchemaConvert(() -> createRegistrySchemaConverter(topic));
            }
            default: {
                throw new IllegalArgumentException("Unsupported converter type: " + type);
            }
        }
    }

    private Converter createRegistrySchemaConverter(String topic) {
        if (schemaRegistry != null) {
            try {
                SchemaFormat format = topicSchemaFormatCache.getIfPresent(topic);
                if (format == null) {
                    format = getSchemaFormat(topic);
                    if (format == null) {
                        throw new RuntimeException("Failed to get schema metadata for topic: " + topic);
                    }
                    topicSchemaFormatCache.put(topic, format);
                }
                switch (format) {
                    case AVRO: {
                        return createAvroConverter(topic);
                    }
                    case PROTOBUF: {
                        return createProtobufConverter(topic);
                    }
                    default: {
                        throw new IllegalArgumentException("Unsupported schema format: " + format);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to get schema metadata for topic: " + topic, e);
            }
        } else {
            throw new RuntimeException("Schema registry is not configured");
        }
    }

    private String getSubjectName(String topic) {
        return topic + "-value";
    }

    private Converter createAvroConverter(String topic) {
        KafkaRecordConvert<GenericRecord> recordConvert = recordConvertMap.computeIfAbsent(AVRO,
            format -> createKafkaAvroRecordConvert(registryUrl));
        return new RegistrySchemaAvroConverter(recordConvert, topic);
    }

    private Converter createProtobufConverter(String topic) {
        KafkaRecordConvert<GenericRecord> recordConvert = recordConvertMap.computeIfAbsent(SchemaFormat.PROTOBUF,
            format -> createKafkaProtobufRecordConvert(registryUrl));
        return new RegistrySchemaAvroConverter(recordConvert, topic);
    }

    @SuppressWarnings("unchecked")
    private KafkaRecordConvert<GenericRecord> createKafkaAvroRecordConvert(String registryUrl) {
        AvroKafkaRecordConvert avroConnectRecordConvert = new AvroKafkaRecordConvert(schemaRegistry);
        avroConnectRecordConvert.configure(Map.of("schema.registry.url", registryUrl), false);
        return avroConnectRecordConvert;
    }

    @SuppressWarnings("unchecked")
    private KafkaRecordConvert<GenericRecord> createKafkaProtobufRecordConvert(String registryUrl) {
        ProtobufKafkaRecordConvert protobufKafkaRecordConvert = new ProtobufKafkaRecordConvert(schemaRegistry);
        protobufKafkaRecordConvert.configure(Map.of("schema.registry.url", registryUrl), false);
        return protobufKafkaRecordConvert;
    }
}
