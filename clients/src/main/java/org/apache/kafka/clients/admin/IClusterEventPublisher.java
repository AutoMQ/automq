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

/**
 * Interface for publishing cluster events to the {@code __automq_cluster_events} internal topic.
 */
public interface IClusterEventPublisher extends AutoCloseable {

    /**
     * Publish a domain event to {@code __automq_cluster_events}.
     *
     * @param type       CloudEvent type (reverse-DNS), e.g. {@code "com.automq.ops.rebalance.summary"}
     * @param source     CloudEvent source URI, e.g. {@code "/automq/broker/0"}
     * @param subject    CloudEvent subject, or {@code null} if not applicable
     * @param dataSchema fully-qualified protobuf message type name
     * @param data       serialized protobuf payload
     */
    void publishEvent(String type, String source, String subject, String dataSchema, byte[] data);

    @Override
    void close();
}
