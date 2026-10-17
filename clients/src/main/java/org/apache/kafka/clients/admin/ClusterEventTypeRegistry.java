/*
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

package org.apache.kafka.clients.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;

/**
 * Maps CloudEvent {@code dataschema} values to their POJO wrappers.
 *
 * <p>Usage:
 * <pre>{@code
 * ClusterEventTypeRegistry.decode(event).ifPresent(obj -> {
 *     if (obj instanceof RebalanceSummaryEventData summary) { ... }
 *     else if (obj instanceof FailoverEventData failover) { ... }
 * });
 * }</pre>
 */
public class ClusterEventTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventTypeRegistry.class);

    private static final Map<String, Function<byte[], Object>> DECODERS = Map.of(
        RebalanceSummaryEventData.DATA_SCHEMA,        b -> RebalanceSummaryEventData.fromByteArray(b),
        RebalancePartitionEventData.DATA_SCHEMA,      b -> RebalancePartitionEventData.fromByteArray(b),
        FailoverEventData.DATA_SCHEMA,                b -> FailoverEventData.fromByteArray(b),
        RequestErrorEventData.DATA_SCHEMA,            b -> RequestErrorEventData.fromByteArray(b),
        OffsetCommitFrequencyEventData.DATA_SCHEMA,   b -> OffsetCommitFrequencyEventData.fromByteArray(b)
    );

    private ClusterEventTypeRegistry() { }

    /**
     * Decode the protobuf payload of a CloudEvent into its POJO wrapper.
     *
     * @return the decoded wrapper, or empty if the dataschema is unknown or the payload is invalid
     */
    public static Optional<Object> decode(CloudEvent event) {
        URI dataSchema = event.getDataSchema();
        CloudEventData data = event.getData();
        if (dataSchema == null || data == null) {
            return Optional.empty();
        }
        Function<byte[], Object> decoder = DECODERS.get(dataSchema.toString());
        if (decoder == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(decoder.apply(data.toBytes()));
        } catch (Exception e) {
            log.debug("Failed to decode CloudEvent payload for dataschema={}", dataSchema, e);
            return Optional.empty();
        }
    }
}
