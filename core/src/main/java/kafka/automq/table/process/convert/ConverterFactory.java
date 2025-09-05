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

package kafka.automq.table.process.convert;

import kafka.automq.table.deserializer.proto.LatestSchemaResolutionResolver;
import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.SchemaFormat;
import kafka.automq.table.process.exception.ProcessorInitializationException;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.server.record.TableTopicConvertType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class ConverterFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConverterFactory.class);

    private static final String VALUE_SUFFIX = "-value";
    private static final String KEY_SUFFIX = "-key";
    private static final String PROTOBUF_TYPE = "PROTOBUF";
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofMinutes(20);
    private static final int MAX_CACHE_SIZE = 10000;

    private final String schemaRegistryUrl;
    private final SchemaRegistryClient client;
    private final Map<String, Converter> converterCache = new ConcurrentHashMap<>();
    private final Cache<String, String> topicSchemaFormatCache = CacheBuilder.newBuilder()
        .expireAfterAccess(CACHE_EXPIRE_DURATION)
        .maximumSize(MAX_CACHE_SIZE)
        .build();

    public ConverterFactory(String registryUrl) {
        this.schemaRegistryUrl = registryUrl;
        if (registryUrl != null && !registryUrl.trim().isEmpty()) {
            this.client = new CachedSchemaRegistryClient(
                registryUrl,
                AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
                List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                null
            );
        } else {
            this.client = null;
        }
    }

    public ConverterFactory(String registryUrl, SchemaRegistryClient client) {
        this.schemaRegistryUrl = registryUrl;
        this.client = client;
    }

    public Converter createKeyConverter(String topic, WorkerConfig config) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("WorkerConfig cannot be null");
        }
        TableTopicConvertType convertType = config.keyConvertType();
        String subject = config.valueSubject();
        String messageName = config.valueMessageFullName();
        return createConverterByType(topic, convertType, subject, messageName, true);
    }

    public Converter createValueConverter(String topic, WorkerConfig config) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("WorkerConfig cannot be null");
        }
        TableTopicConvertType convertType = config.valueConvertType();
        String subject = config.valueSubject();
        String messageName = config.valueMessageFullName();
        return createConverterByType(topic, convertType, subject, messageName, false);
    }

    private Converter createConverterByType(String topic, TableTopicConvertType convertType, String subjectName, String messageName, boolean isKey) {
        switch (convertType) {
            case RAW:
                return new RawConverter();
            case STRING:
                return new StringConverter();
            case BY_SCHEMA_ID:
                return createForSchemaId(topic, isKey);
            case BY_LATEST_SCHEMA:
                return createForSubjectName(topic, subjectName, messageName, isKey);
            default:
                throw new IllegalArgumentException("Unsupported convert type: " + convertType);
        }
    }

    public Converter createForSchemaId(String topic, boolean isKey) {
        if (client == null) {
            throw new ProcessorInitializationException("Schema Registry client is not initialized");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }

        return new LazyConverter(() -> {
            String subject = getSubjectName(topic, isKey);
            String schemaType = getSchemaType(subject);
            SchemaFormat format = SchemaFormat.fromString(schemaType);
            return converterCache.computeIfAbsent(format.name(), format1 -> createConverterForFormat(format1, isKey));
        });
    }

    public Converter createForSubjectName(String topic, String subjectName, String messageFullName, boolean isKey) {
        String subject = subjectName != null ? subjectName : getSubjectName(topic);
        return new LazyConverter(() -> {
            String schemaType = getSchemaType(subject);
            if (!PROTOBUF_TYPE.equals(schemaType)) {
                throw new ProcessorInitializationException(
                    String.format("by_subject_name is only supported for PROTOBUF, but got %s for subject %s", schemaType, subject));
            }

            String cacheKey = schemaType + "-" + subject + "-" + (messageFullName == null ? "" : messageFullName);
            return converterCache.computeIfAbsent(cacheKey, key -> {
                var resolver = new LatestSchemaResolutionResolver(client, subject, messageFullName);
                return new ProtobufRegistryConverter(client, schemaRegistryUrl, resolver, isKey);
            });
        });
    }

    private String getSchemaType(String subject) {
        if (client == null) {
            throw new ProcessorInitializationException("Schema Registry client is not available");
        }

        String schemaType = topicSchemaFormatCache.getIfPresent(subject);
        if (schemaType == null) {
            try {
                var metadata = client.getLatestSchemaMetadata(subject);
                if (metadata == null) {
                    throw new ProcessorInitializationException("No schema found for subject: " + subject);
                }
                schemaType = metadata.getSchemaType();
                if (schemaType != null) {
                    topicSchemaFormatCache.put(subject, schemaType);
                }
            } catch (IOException e) {
                LOGGER.error("IO error while fetching schema metadata for subject '{}'", subject, e);
                throw new ProcessorInitializationException("Failed to fetch schema metadata for subject: " + subject, e);
            } catch (RestClientException e) {
                LOGGER.error("Schema Registry error for subject '{}'", subject, e);
                throw new ProcessorInitializationException("Schema Registry error for subject: " + subject, e);
            }
        }
        return schemaType;
    }

    private String getSubjectName(String topic) {
        return topic + VALUE_SUFFIX;
    }

    private String getSubjectName(String topic, boolean isKey) {
        return topic + (isKey ? KEY_SUFFIX : VALUE_SUFFIX);
    }

    private Converter createConverterForFormat(String format, boolean isKey) {
        LOGGER.info("Creating new converter for format: {}", format);
        SchemaFormat schemaFormat = SchemaFormat.fromString(format);
        switch (schemaFormat) {
            case AVRO:
                return new AvroRegistryConverter(client, schemaRegistryUrl, isKey);
            case PROTOBUF:
                return new ProtobufRegistryConverter(client, schemaRegistryUrl, isKey);
            default:
                LOGGER.error("Unsupported schema format '{}'", format);
                throw new ProcessorInitializationException("Unsupported schema format: " + format);
        }
    }

}
