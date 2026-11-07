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

package kafka.autobalancer.metricsreporter.metric;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DerivatorTest {

    @Test
    public void testDerive() {
        Derivator derivator = new Derivator();
        Assertions.assertEquals(0.0, derivator.derive(1, 2, true));
        derivator.reset();
        Assertions.assertEquals(2.0, derivator.derive(1, 2, false));
        derivator.reset();
        Assertions.assertEquals(0,  derivator.derive(0, 2, true));
        derivator.reset();
        Assertions.assertEquals(0,  derivator.derive(0, 2, false));
        Assertions.assertEquals(0,  derivator.derive(0, 2));
        Assertions.assertEquals(0,  derivator.derive(2, 2));
        Assertions.assertEquals(0,  derivator.derive(2, 4));
        Assertions.assertEquals(4,  derivator.derive(3, 8));
    }
}
