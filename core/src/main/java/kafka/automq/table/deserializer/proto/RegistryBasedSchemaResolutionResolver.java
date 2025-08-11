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

import org.apache.kafka.common.errors.SerializationException;

import com.automq.stream.utils.Time;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.automq.table.deserializer.proto.schema.MessageIndexes;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of SchemaResolutionResolver that retrieves the latest schema from Schema Registry by subject name.
 * This implementation includes caching mechanism to avoid frequent registry queries.
 * Cache entries are refreshed every 5 minutes.
 */
public class RegistryBasedSchemaResolutionResolver implements SchemaResolutionResolver {

    private static final long CACHE_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
    private static final MessageIndexes DEFAULT_INDEXES = new MessageIndexes(Collections.singletonList(0));

    private final SchemaRegistryClient schemaRegistry;
    private final ConcurrentHashMap<String, RegistryBasedSchemaResolutionResolver.CachedSchemaInfo> schemaCache = new ConcurrentHashMap<>();
    private final Time time;

    public RegistryBasedSchemaResolutionResolver(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
        time = Time.SYSTEM;
    }

    @Override
    public SchemaResolution resolve(String topic, byte[] payload) {
        if (payload == null) {
            throw new SerializationException("Payload cannot be null");
        }

        String subject = getSubjectName(topic);
        RegistryBasedSchemaResolutionResolver.CachedSchemaInfo cachedInfo = getCachedSchemaInfo(subject);

        return new SchemaResolution(cachedInfo.schemaId, DEFAULT_INDEXES, payload);
    }

    private RegistryBasedSchemaResolutionResolver.CachedSchemaInfo getCachedSchemaInfo(String subject) {
        long currentTime = time.milliseconds();

        return schemaCache.compute(subject, (key, existing) -> {
            // If we have existing data and it's still fresh, use it
            if (existing != null && currentTime - existing.lastUpdated <= CACHE_REFRESH_INTERVAL_MS) {
                return existing;
            }

            // Try to get fresh data from registry
            try {
                SchemaMetadata latestSchema = schemaRegistry.getLatestSchemaMetadata(subject);
                return new RegistryBasedSchemaResolutionResolver.CachedSchemaInfo(latestSchema.getId(), currentTime);
            } catch (IOException | RestClientException e) {
                // If we have existing cached data (even if expired), use it as fallback
                if (existing != null) {
                    // Log warning but continue with stale data
                    System.err.println("Warning: Failed to refresh schema for subject " + subject +
                        ", using cached data from " +
                        new java.util.Date(existing.lastUpdated) + ": " + e.getMessage());
                    return existing;
                }
                // No cached data and fresh fetch failed - this is a hard error
                throw new SerializationException("Error retrieving schema for subject " + subject +
                    " and no cached data available", e);
            }
        });
    }

    private String getSubjectName(String topic) {
        // Follow the Confluent naming convention: <topic>-value or <topic>-key
        return topic + "-value";
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
