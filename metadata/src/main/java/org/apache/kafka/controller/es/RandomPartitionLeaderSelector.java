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

import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class RandomPartitionLeaderSelector implements PartitionLeaderSelector {
    private final List<BrokerRegistration> aliveBrokers;
    private int selectNextIndex = 0;

    public RandomPartitionLeaderSelector(List<BrokerRegistration> aliveBrokers) {
        this.aliveBrokers = new ArrayList<>(aliveBrokers);
        Collections.shuffle(this.aliveBrokers);
    }

    @Override
    public Optional<BrokerRegistration> select(PartitionRegistration partition, Predicate<BrokerRegistration> predicate) {
        for (int i = 0; i < aliveBrokers.size(); i++) {
            BrokerRegistration broker = aliveBrokers.get(selectNextIndex);
            selectNextIndex = (selectNextIndex + 1) % aliveBrokers.size();
            if (predicate.test(broker)) {
                return Optional.of(broker);
            }
        }
        return Optional.empty();
    }
}
