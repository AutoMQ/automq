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
package org.apache.kafka.coordinator.group.api.assignor;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Server-side partition assignor for consumer groups used by the GroupCoordinator.
 *
 * The new consumer group protocol is in preview so this interface is considered
 * unstable until Apache Kafka 4.0.
 */
@InterfaceStability.Unstable
public interface ConsumerGroupPartitionAssignor extends PartitionAssignor {
}
