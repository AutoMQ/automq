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
import kafka.automq.table.process.exception.ProcessorInitializationException;
import kafka.automq.table.transformer.SchemaFormat;

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
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

/**
 * Factory for creating and managing {@link Converter} instances.
 *
 * <p>This factory provides a centralized mechanism for creating converters for different
 * data formats. It caches converter instances by format to avoid redundant object
 * creation and expensive re-initialization.</p>
 *
 * <p>It supports lazy initialization of converters to defer resource-intensive
 * setup until a converter is actually needed.</p>
 */
public class RegistryConverterFactory {
    private static final Logger log = LoggerFactory.getLogger(RegistryConverterFactory.class);

    private String schemaRegistryUrl;
    private SchemaRegistryClient client;

    private final Map<String, Converter> converterCache = new ConcurrentHashMap<>();

    // Cache topic schema format to avoid repeated Schema Registry queries
    private final Cache<String, String> topicSchemaFormatCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(20))
        .maximumSize(10000)
        .build();


    public RegistryConverterFactory(String registryUrl) {
        this.schemaRegistryUrl = registryUrl;
        if (registryUrl != null) {
            client = new CachedSchemaRegistryClient(
                registryUrl,
                AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
                List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                null
            );
        }
    }

    /**
     * Gets or creates a converter for a specific topic.
     *
     * <p>This method determines the schema format for the topic and returns a cached
     * converter for that format. Multiple topics with the same format share the same
     * converter instance, which improves resource efficiency.</p>
     */
    public Converter getOrCreate(String topic, boolean useLatestSchema) {
        try {
            return new LazyConverter(() -> {
                // 1. Try to get schema format from cache first
                String schemaType = topicSchemaFormatCache.getIfPresent(topic);
                try {
                    if (schemaType == null) {
                        String subject = getSubjectName(topic);
                        schemaType = client.getLatestSchemaMetadata(subject).getSchemaType();

                        // Cache the result for future queries
                        topicSchemaFormatCache.put(topic, schemaType);
                        log.debug("Cached schema type '{}' for topic '{}'", schemaType, topic);
                    } else {
                        log.debug("Cache hit: schema type '{}' for topic '{}'", schemaType, topic);
                    }
                } catch (IOException | io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException e) {
                    log.error("Failed to get schema metadata for topic '{}' due to Schema Registry error", topic, e);
                    throw new ProcessorInitializationException("Failed to get schema metadata for topic: " + topic, e);
                }

                if (useLatestSchema && "PROTOBUF".equals(schemaType)) {
                    String subject = getSubjectName(topic);
                    String cacheKey = schemaType + "-" + subject;
                    return converterCache.computeIfAbsent(cacheKey, v -> new ProtobufRegistryConverter(client, schemaRegistryUrl, new LatestSchemaResolutionResolver(client, subject)));
                }

                if (!useLatestSchema) {
                    SchemaFormat cacheKey = SchemaFormat.fromString(schemaType);
                    return converterCache.computeIfAbsent(cacheKey.name(), this::createConverterForFormat);
                }
                throw new RuntimeException("Unsupported schema format: " + schemaType);
            });
        } catch (Exception e) {
            log.error("An unexpected error occurred while getting converter for topic '{}'", topic, e);
            throw new ProcessorInitializationException("Failed to get converter for topic: " + topic, e);
        }
    }

    private String getSubjectName(String topic) {
        return topic + "-value";
    }

    /**
     * Creates a concrete converter instance for a given format.
     * This method is called by the {@link LazyConverter} supplier.
     */
    private Converter createConverterForFormat(String format) {
        log.info("Creating new converter for format: {}", format);
        SchemaFormat schemaFormat = SchemaFormat.fromString(format);
        switch (schemaFormat) {
            case AVRO:
                return new AvroRegistryConverter(client, schemaRegistryUrl);
            case PROTOBUF:
                return new ProtobufRegistryConverter(client, schemaRegistryUrl);

            default:
                log.error("Unsupported schema format '{}'", format);
                throw new RuntimeException("Unsupported schema format: " + format);
        }
    }
}
