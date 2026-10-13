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

package org.apache.kafka.metadata.storage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies that newly formatted clusters finalize sharding support without a feature override. */
@Tag("S3Unit")
public class WALShardingBootstrapTest {
    /** Formatting a new cluster at the default release version enables sharded WAL writes immediately. */
    @Test
    public void testDefaultFormattingEnablesSharding(@TempDir Path directory) throws Exception {
        Formatter formatter = new Formatter().setNodeId(0).setClusterId(Uuid.randomUuid().toString())
            .setControllerListenerName("CONTROLLER").setMetadataLogDirectory(directory.toString())
            .addDirectory(directory.toString());
        formatter.run();
        FeatureLevelRecord feature = formatter.bootstrapMetadata().records().stream()
            .map(record -> record.message()).filter(record -> record instanceof FeatureLevelRecord)
            .map(record -> (FeatureLevelRecord) record)
            .filter(record -> AutoMQVersion.FEATURE_NAME.equals(record.name())).findFirst().orElseThrow();
        assertEquals(AutoMQVersion.V6.featureLevel(), feature.featureLevel());
    }
}
