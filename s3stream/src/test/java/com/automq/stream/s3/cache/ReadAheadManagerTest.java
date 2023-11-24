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

package com.automq.stream.s3.cache;

import com.automq.stream.utils.Threads;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ReadAheadManagerTest {

    @Test
    public void testUpdateAgent() {
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        ReadAheadManager manager = new ReadAheadManager(10, blockCache);
        manager.updateReadProgress(233L, 0);
        Assertions.assertNull(manager.getReadAheadAgent(233L, 0));
        manager.getOrCreateReadAheadAgent(233L, 0);
        ReadAheadAgent agent = manager.getReadAheadAgent(233L, 0);
        Assertions.assertNotNull(agent);
        agent.updateReadAheadResult(1024, 1224);
        manager.updateReadResult(233L, 0, 512, 612);
        Assertions.assertEquals(1024, agent.getReadAheadOffset());
        Assertions.assertEquals(1224, agent.getLastReadAheadSize());
        Assertions.assertEquals(512, agent.getLastReadOffset());
        Assertions.assertEquals(612, agent.getLastReadSize());
        Assertions.assertEquals(0, agent.getBytesPerMillis());

        Threads.sleep(1000);
        manager.updateReadProgress(233L, 512);
        Assertions.assertEquals(0.306, agent.getBytesPerMillis(), 0.01);
        Assertions.assertEquals(122, agent.getNextReadAheadSize(), 5);
        manager.updateReadResult(233L, 512, 1024, 612);
        agent.updateReadAheadResult(2048, 1224);

        manager.onCacheEvict(233L, 0, 256, 257);
        manager.onCacheEvict(233L, 768, 1345, 712);
        manager.onCacheEvict(233L, 1678, 1789, 299);

        Assertions.assertEquals(70, agent.getNextReadAheadSize(), 1);
    }

    @Test
    public void testReadAheadAgents() {
        BlockCache blockCache = Mockito.mock(BlockCache.class);
        ReadAheadManager manager = new ReadAheadManager(10, blockCache);
        manager.updateReadProgress(233L, 0);
        manager.getOrCreateReadAheadAgent(233L, 0);
        manager.updateReadResult(233L, 0, 10, 10);

        manager.updateReadProgress(233L, 10);
        manager.getOrCreateReadAheadAgent(233L, 10);
        manager.updateReadResult(233L, 10, 20, 10);

        manager.updateReadProgress(233L, 20);
        manager.getOrCreateReadAheadAgent(233L, 20);
        manager.updateReadResult(233L, 20, 50, 30);

        Assertions.assertEquals(1, manager.getReadAheadAgents().size());
    }
}
