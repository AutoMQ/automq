/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.availability.broker;

import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Filters retained Broker-executed actions down to work addressed to this Broker and still before deadline.
 */
public class BrokerActionReceiver {
    private final int brokerId;

    public BrokerActionReceiver(int brokerId) {
        this.brokerId = brokerId;
    }

    public List<RecoveryAction> pollActions(List<RecoveryAction> retainedActions, long nowMs) {
        return retainedActions.stream()
            .filter(action -> action.getExecutorBrokerId() == brokerId)
            .filter(action -> nowMs <= action.getDeadlineMs())
            .collect(Collectors.toList());
    }
}
