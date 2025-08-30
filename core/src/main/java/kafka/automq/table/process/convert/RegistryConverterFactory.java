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

public class RegistryConverterFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryConverterFactory.class);

    private final String schemaRegistryUrl;
    private final SchemaRegistryClient client;
    private final Map<String, Converter> converterCache = new ConcurrentHashMap<>();
    private final Cache<String, String> topicSchemaFormatCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(20))
        .maximumSize(10000)
        .build();

    public RegistryConverterFactory(String registryUrl) {
        this.schemaRegistryUrl = registryUrl;
        if (registryUrl != null) {
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

    public RegistryConverterFactory(String registryUrl, SchemaRegistryClient client) {
        this.schemaRegistryUrl = registryUrl;
        this.client = client;
    }

    public Converter createForSchemaId(String topic) {
        return new LazyConverter(() -> {
            String subject = getSubjectName(topic);
            String schemaType = getSchemaType(subject);
            SchemaFormat format = SchemaFormat.fromString(schemaType);
            return converterCache.computeIfAbsent(format.name(), this::createConverterForFormat);
        });
    }

    public Converter createForSubjectName(String topic, String subjectName, String messageFullName) {
        String subject = subjectName != null ? subjectName : getSubjectName(topic);
        return new LazyConverter(() -> {
            String schemaType = getSchemaType(subject);
            if (!"PROTOBUF".equals(schemaType)) {
                throw new ProcessorInitializationException(
                    String.format("by_subject_name is only supported for PROTOBUF, but got %s for subject %s", schemaType, subject));
            }

            String cacheKey = schemaType + "-" + subject + "-" + (messageFullName == null ? "" : messageFullName);
            return converterCache.computeIfAbsent(cacheKey, key -> {
                var resolver = new LatestSchemaResolutionResolver(client, subject, messageFullName);
                return new ProtobufRegistryConverter(client, schemaRegistryUrl, resolver);
            });
        });
    }

    private String getSchemaType(String subject) {
        String schemaType = topicSchemaFormatCache.getIfPresent(subject);
        if (schemaType == null) {
            try {
                schemaType = client.getLatestSchemaMetadata(subject).getSchemaType();
                topicSchemaFormatCache.put(subject, schemaType);
            } catch (IOException | RestClientException e) {
                LOGGER.error("Failed to get schema metadata for subject '{}' due to Schema Registry error", subject, e);
                throw new ProcessorInitializationException("Failed to get schema metadata for subject: " + subject, e);
            }
        }
        return schemaType;
    }

    private String getSubjectName(String topic) {
        return topic + "-value";
    }

    private Converter createConverterForFormat(String format) {
        LOGGER.info("Creating new converter for format: {}", format);
        SchemaFormat schemaFormat = SchemaFormat.fromString(format);
        switch (schemaFormat) {
            case AVRO:
                return new AvroRegistryConverter(client, schemaRegistryUrl);
            case PROTOBUF:
                return new ProtobufRegistryConverter(client, schemaRegistryUrl);
            default:
                LOGGER.error("Unsupported schema format '{}'", format);
                throw new RuntimeException("Unsupported schema format: " + format);
        }
    }
}
