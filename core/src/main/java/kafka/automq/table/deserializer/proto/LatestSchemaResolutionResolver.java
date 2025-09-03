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

import kafka.automq.table.deserializer.SchemaResolutionResolver;
import kafka.automq.table.deserializer.proto.schema.MessageIndexes;

import org.apache.kafka.common.errors.SerializationException;

import com.automq.stream.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Implementation of SchemaResolutionResolver that retrieves the latest schema from Schema Registry by subject name.
 * This implementation includes caching mechanism to avoid frequent registry queries.
 * Cache entries are refreshed every 5 minutes.
 */
public class LatestSchemaResolutionResolver implements SchemaResolutionResolver {
    private static final Logger log = LoggerFactory.getLogger(LatestSchemaResolutionResolver.class);

    private static final long CACHE_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
    private static final MessageIndexes DEFAULT_INDEXES = new MessageIndexes(Collections.singletonList(0));

    private final SchemaRegistryClient schemaRegistry;

    private volatile LatestSchemaResolutionResolver.CachedSchemaInfo schemaCache;

    private final Time time;
    private final String subject;
    private final String messageFullName;

    public LatestSchemaResolutionResolver(SchemaRegistryClient schemaRegistry, String subject) {
        this(schemaRegistry, subject, null);
    }

    public LatestSchemaResolutionResolver(SchemaRegistryClient schemaRegistry, String subject, String messageFullName) {
        this.schemaRegistry = schemaRegistry;
        this.time = Time.SYSTEM;
        this.subject = subject;
        this.messageFullName = messageFullName;
    }

    @Override
    public SchemaResolution resolve(String topic, ByteBuffer payload) {
        if (payload == null) {
            throw new SerializationException("Payload cannot be null");
        }
        LatestSchemaResolutionResolver.CachedSchemaInfo cachedInfo = getCachedSchemaInfo(subject);

        if (messageFullName == null) {
            return new SchemaResolution(cachedInfo.schemaId, DEFAULT_INDEXES, payload);
        } else {
            return new SchemaResolution(cachedInfo.schemaId, subject, messageFullName, payload);
        }
    }

    @Override
    public int getSchemaId(String topic, ByteBuffer payload) {
        LatestSchemaResolutionResolver.CachedSchemaInfo cachedInfo = getCachedSchemaInfo(subject);
        return cachedInfo.schemaId;
    }

    private LatestSchemaResolutionResolver.CachedSchemaInfo getCachedSchemaInfo(String subject) {
        long currentTime = time.milliseconds();
        // First check (no lock)
        if (schemaCache == null || currentTime - schemaCache.lastUpdated > CACHE_REFRESH_INTERVAL_MS) {
            synchronized (this) {
                // Second check (with lock)
                if (schemaCache == null || currentTime - schemaCache.lastUpdated > CACHE_REFRESH_INTERVAL_MS) {
                    try {
                        SchemaMetadata latestSchema = schemaRegistry.getLatestSchemaMetadata(subject);
                        schemaCache = new LatestSchemaResolutionResolver.CachedSchemaInfo(latestSchema.getId(), currentTime);
                    } catch (IOException | RestClientException e) {
                        if (schemaCache == null) {
                            // No cached data and fresh fetch failed - this is a hard error
                            throw new SerializationException("Error retrieving schema for subject " + subject +
                                " and no cached data available", e);
                        } else {
                            log.warn("Failed to retrieve latest schema for subject '{}'. Using stale cache.", subject, e);
                        }
                    }
                }
            }
        }
        return schemaCache;
    }


    private static class CachedSchemaInfo {
        final int schemaId;
        final long lastUpdated;

        CachedSchemaInfo(int schemaId, long lastUpdated) {
            this.schemaId = schemaId;
            this.lastUpdated = lastUpdated;
        }
    }
}
