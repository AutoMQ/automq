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

package org.apache.kafka.controller.es;

import java.util.Map;
import org.apache.kafka.common.errors.PolicyViolationException;

/**
 * <p>A policy on create partition requests.
 */
public class ElasticCreatePartitionPolicy implements CreatePartitionPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.replicas() == null || requestMetadata.replicas().size() != 1) {
            throw new PolicyViolationException("There should be exactly one replica, but got " + requestMetadata.replicas().size() + " replicas");
        }
        if (requestMetadata.isrs() == null || requestMetadata.isrs().size() != 1) {
            throw new PolicyViolationException("There should be exactly one in-sync replica, but got " + requestMetadata.isrs().size() + " in-sync replicas");
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
