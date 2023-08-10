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
package kafka.server.es;

import java.util.Map;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

/**
 * <p>A policy on create topic requests.
 */
public class ElasticCreateTopicPolicy implements CreateTopicPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.replicationFactor() != null && requestMetadata.replicationFactor() != 1) {
            throw new PolicyViolationException("Replication factor must be 1");
        }
        if (requestMetadata.replicasAssignments() != null) {
            requestMetadata.replicasAssignments().forEach((key, value) -> {
                if (value.size() != 1) {
                    throw new PolicyViolationException("Partition " + key + " can only have exactly one replica, but got " + value.size() + " replicas");
                }
            });
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
