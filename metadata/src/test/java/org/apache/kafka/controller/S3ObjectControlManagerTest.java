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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.S3ObjectControlManager;
import org.apache.kafka.metadata.stream.S3Config;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(40)
public class S3ObjectControlManagerTest {

    private static final String CLUSTER = "kafka-on-S3_cluster";
    private static final String S3_REGION = "us-east-1";
    private static final String S3_BUCKET = "kafka-on-S3-bucket";

    private static final S3Config S3_CONFIG = new S3Config(S3_REGION, S3_BUCKET);
    private S3ObjectControlManager manager;

    private QuorumController controller;

    @BeforeEach
    private void setUp() {
        controller = Mockito.mock(QuorumController.class);
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        manager = new S3ObjectControlManager(controller, registry, logContext, CLUSTER, S3_CONFIG);

    }

}
