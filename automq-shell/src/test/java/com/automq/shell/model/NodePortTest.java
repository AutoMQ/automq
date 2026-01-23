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

package com.automq.shell.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NodePortTest {

    @Test
    public void testNodePortFromYaml() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();

        try (InputStream is = getClass().getResourceAsStream("/test-topo.yaml")) {
            ClusterTopology topo = mapper.readValue(is, ClusterTopology.class);

            // controller 0 has port: 19092
            assertEquals(19092, topo.getControllers().get(0).getPort());
            // controller 1 has no port set, should use default 9092
            assertEquals(9092, topo.getControllers().get(1).getPort());
            // broker has port: 29092
            assertEquals(29092, topo.getBrokers().get(0).getPort());
        }
    }

    @Test
    public void testNodeDefaultPort() {
        Node node = new Node();
        assertEquals(9092, node.getPort());
    }
}
