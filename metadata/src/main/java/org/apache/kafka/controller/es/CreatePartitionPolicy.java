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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.List;
import java.util.Objects;

/**
 * <p>An interface for enforcing a policy on create partition requests.
 */
public interface CreatePartitionPolicy extends Configurable, AutoCloseable {
    /**
     * Class containing the create request parameters.
     */
    class RequestMetadata {
        private final List<Integer> replicas;
        private final List<Integer> isrs;

        /**
         * Create an instance of this class with the provided parameters.
         * <p>
         * This constructor is public to make testing of <code>CreatePartitionPolicy</code> implementations easier.
         *
         * @param replicas replica assignments of the partition
         * @param isrs     in-sync replica assignments of the partition
         */
        public RequestMetadata(List<Integer> replicas, List<Integer> isrs) {
            this.replicas = replicas;
            this.isrs = isrs;
        }

        public List<Integer> replicas() {
            return replicas;
        }

        public List<Integer> isrs() {
            return isrs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isrs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CreatePartitionPolicy.RequestMetadata other = (CreatePartitionPolicy.RequestMetadata) o;
            return Objects.equals(replicas, other.replicas) &&
                Objects.equals(isrs, other.isrs);
        }

        @Override
        public String toString() {
            return "CreatePartitionPolicy.RequestMetadata(replicas=" + replicas +
                ", isrs=" + isrs + ")";
        }
    }

    /**
     * Validate the request parameters and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the parameters for the provided partition do not satisfy this policy.
     * <p>
     *
     * @param requestMetadata the create partition request parameters for the provided partition.
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validate(CreatePartitionPolicy.RequestMetadata requestMetadata) throws PolicyViolationException;
}
