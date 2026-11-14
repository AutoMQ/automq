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
package org.apache.kafka.common.security.fips;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class FipsRuntimeTest {
    @Test
    void runtimeIsDisabledBeforeEnterpriseBootstrap() {
        assertFalse(FipsRuntime.enabled());
    }

    @Test
    void enabledModeRequiresFipsHost() {
        assumeFalse(hostFipsEnabled());
        IllegalStateException error = assertThrows(IllegalStateException.class, FipsRuntime::initialize);
        assertTrue(error.getMessage().contains("fips_enabled=1"));
        assertFalse(FipsRuntime.enabled());
    }

    private boolean hostFipsEnabled() {
        try {
            return "1".equals(Files.readString(Path.of("/proc/sys/crypto/fips_enabled")).trim());
        } catch (Exception ignored) {
            return false;
        }
    }
}
