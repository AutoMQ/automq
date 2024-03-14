/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
        Assertions.assertEquals(0, agent.getBytePerSecond());

        Threads.sleep(1000);
        manager.updateReadProgress(233L, 512);
        Assertions.assertEquals(306, agent.getBytePerSecond(), 10);
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
