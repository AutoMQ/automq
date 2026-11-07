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

package kafka.automq.backpressure;

/**
 * Represents the load level of the system.
 * {@link BackPressureManager} will take actions based on the load level.
 * Note: It MUST be ordered by the severity.
 */
public enum LoadLevel {
    /**
     * The system is in a normal state.
     */
    NORMAL {
        @Override
        public void regulate(Regulator regulator) {
            regulator.increase();
        }
    },
    /**
     * The system is in a high load state, and some actions should be taken to reduce the load.
     */
    HIGH {
        @Override
        public void regulate(Regulator regulator) {
            regulator.decrease();
        }
    };

    /**
     * Take actions based on the load level.
     */
    public abstract void regulate(Regulator regulator);
}
