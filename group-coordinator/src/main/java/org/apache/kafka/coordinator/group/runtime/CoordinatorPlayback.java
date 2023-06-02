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
package org.apache.kafka.coordinator.group.runtime;

/**
 * The CoordinatorPlayback interface. This interface is used to replay
 * records to the coordinator in order to update its state. This is
 * also used to rebuild a coordinator state from scratch by replaying
 * all records stored in the partition.
 *
 * @param <U> The type of the record.
 */
public interface CoordinatorPlayback<U> {

    /**
     * Applies the given record to this object.
     *
     * @param record A record.
     * @throws RuntimeException if the record can not be applied.
     */
    void replay(U record) throws RuntimeException;
}
