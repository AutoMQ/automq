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

package org.apache.kafka.controller.availability;

public class ProtectionStateManager {
    private ProtectionState current = ProtectionState.none();

    public ProtectionState update(FaultAttribution attribution, long nowMs) {
        if (attribution.type() == FaultAttributionType.GLOBAL_SHARED_STORAGE_SUSPECT) {
            if (!current.globalProtected()) {
                current = new ProtectionState(true, BlockedReason.GLOBAL_PROTECTION, nowMs);
            }
            return current;
        }
        current = ProtectionState.none();
        return current;
    }

    public ProtectionState current() {
        return current;
    }
}
