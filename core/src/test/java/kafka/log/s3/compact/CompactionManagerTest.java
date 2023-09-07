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

package kafka.log.s3.compact;

import kafka.log.s3.memory.MemoryMetadataManager;
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.server.KafkaConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CompactionManagerTest {
    private static final int BROKER0 = 0;
    private CompactionManager compactionManager;

    @BeforeEach
    public void setUp() {
        // mock parameters for CompactionManager
        KafkaConfig config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.brokerId()).thenReturn(BROKER0);
        StreamMetadataManager streamMetadataManager = Mockito.mock(StreamMetadataManager.class);
        this.compactionManager = new CompactionManager(config, new MemoryMetadataManager(), streamMetadataManager, new MemoryS3Operator());
    }

    // Test compacting multiple WAL objects
    @Test
    public void testCompact() {

    }
}
