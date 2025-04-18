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

package org.apache.kafka.controller.stream;

import org.apache.kafka.controller.BrokerControlState;

public enum NodeState {
    /**
     * The node is active and can handle requests.
     */
    ACTIVE,
    /**
     * The node is shut down and cannot handle requests.
     */
    FENCED,
    /**
     * The node is shutting down in a controlled manner.
     * Note: In AutoMQ, this state is different from {@link BrokerControlState#CONTROLLED_SHUTDOWN}. In some cases,
     * a node in {@link BrokerControlState#FENCED} state may still be shutting down in a controlled manner.
     */
    CONTROLLED_SHUTDOWN,
    /**
     * The state of the node is unknown, possibly because it has not yet registered.
     */
    UNKNOWN
}
