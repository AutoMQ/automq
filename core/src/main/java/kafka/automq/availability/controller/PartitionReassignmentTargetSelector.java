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

package kafka.automq.availability.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;

/**
 * Selects a reassignment destination from Controller runtime broker eligibility, excluding the current target.
 */
public class PartitionReassignmentTargetSelector {
    private final Random random;

    public PartitionReassignmentTargetSelector() {
        this(new Random());
    }

    public PartitionReassignmentTargetSelector(Random random) {
        this.random = random;
    }

    public OptionalInt select(int currentBrokerId, List<CandidateBroker> candidates) {
        List<CandidateBroker> eligible = eligibleTargets(currentBrokerId, candidates);
        Collections.shuffle(eligible, random);
        return eligible.stream()
            .findFirst()
            .map(candidate -> OptionalInt.of(candidate.brokerId()))
            .orElseGet(OptionalInt::empty);
    }

    public static List<CandidateBroker> eligibleTargets(int currentBrokerId, List<CandidateBroker> candidates) {
        return candidates.stream()
            .filter(CandidateBroker::unfenced)
            .filter(CandidateBroker::dataPathEligible)
            .filter(candidate -> candidate.brokerId() != currentBrokerId)
            .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
    }

    public static class CandidateBroker {
        private final int brokerId;
        private final boolean unfenced;
        private final boolean dataPathEligible;

        public CandidateBroker(int brokerId, boolean unfenced, boolean dataPathEligible) {
            this.brokerId = brokerId;
            this.unfenced = unfenced;
            this.dataPathEligible = dataPathEligible;
        }

        public int brokerId() {
            return brokerId;
        }

        public boolean unfenced() {
            return unfenced;
        }

        public boolean dataPathEligible() {
            return dataPathEligible;
        }
    }
}
