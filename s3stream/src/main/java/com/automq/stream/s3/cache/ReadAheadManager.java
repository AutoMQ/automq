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

import com.automq.stream.utils.LogContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class ReadAheadManager implements BlockCache.CacheEvictListener {
    private static final Logger LOGGER = new LogContext("[S3BlockCache] ").logger(ReadAheadManager.class);
    private static final Integer MAX_READ_AHEAD_AGENT_NUM = 1024;
    // <streamId, <lastReadOffset, ReadAheadAgent>>
    private final Map<Long, NavigableMap<Long, ReadAheadAgent>> readAheadAgentMap;
    private final LRUCache<ReadAheadAgent, Void> readAheadAgentLRUCache = new LRUCache<>();
    private final int dataBlockSize;
    private final BlockCache blockCache;

    public ReadAheadManager(int dataBlockSize, BlockCache blockCache) {
        this.dataBlockSize = dataBlockSize;
        this.readAheadAgentMap = new ConcurrentHashMap<>();
        this.blockCache = blockCache;
        this.blockCache.registerListener(this);
    }

    public void updateReadResult(long streamId, long startOffset, long endOffset, int size) {
        NavigableMap<Long, ReadAheadAgent> agentMap = readAheadAgentMap.get(streamId);
        if (agentMap != null) {
            synchronized (agentMap) {
                ReadAheadAgent agent = agentMap.get(startOffset);
                if (agent == null) {
                    return;
                }
                readAheadAgentLRUCache.remove(agent);
                agent.updateReadResult(startOffset, endOffset, size);
                agentMap.remove(startOffset);
                agentMap.put(endOffset, agent);
                readAheadAgentLRUCache.put(agent, null);
            }
        }
    }

    public void updateReadProgress(long streamId, long startOffset) {
        NavigableMap<Long, ReadAheadAgent> agentMap = readAheadAgentMap.get(streamId);
        if (agentMap != null) {
            synchronized (agentMap) {
                ReadAheadAgent agent = agentMap.get(startOffset);
                if (agent == null) {
                    return;
                }
                agent.updateReadProgress(startOffset);
                readAheadAgentLRUCache.touch(agent);
            }
        }
    }

    public ReadAheadAgent getReadAheadAgent(long streamId, long startOffset) {
        NavigableMap<Long, ReadAheadAgent> agentMap = readAheadAgentMap.get(streamId);
        if (agentMap != null) {
            synchronized (agentMap) {
                ReadAheadAgent agent = agentMap.get(startOffset);
                if (agent != null) {
                    readAheadAgentLRUCache.touch(agent);
                }
                return agent;
            }
        }
        return null;
    }

    public ReadAheadAgent getOrCreateReadAheadAgent(long streamId, long startOffset) {
        NavigableMap<Long, ReadAheadAgent> agentMap = readAheadAgentMap.computeIfAbsent(streamId, k -> new TreeMap<>());
        synchronized (agentMap) {
            while (readAheadAgentLRUCache.size() > MAX_READ_AHEAD_AGENT_NUM) {
                Map.Entry<ReadAheadAgent, Void> entry = readAheadAgentLRUCache.pop();
                if (entry == null) {
                    LOGGER.error("read ahead agent num exceed limit, but no agent can be evicted");
                    return null;
                }
                ReadAheadAgent agent = entry.getKey();
                agentMap.remove(agent.getLastReadOffset());
                LOGGER.info("evict read ahead agent for stream={}, startOffset={}", agent.getStreamId(), agent.getLastReadOffset());
            }
            return agentMap.computeIfAbsent(startOffset, k -> {
                ReadAheadAgent agent = new ReadAheadAgent(dataBlockSize, streamId, k);
                readAheadAgentLRUCache.put(agent, null);
                LOGGER.info("put read ahead agent for stream={}, startOffset={}, total agent num={}", agent.getStreamId(), agent.getLastReadOffset(), readAheadAgentLRUCache.size());
                return agent;
            });
        }
    }

    Set<ReadAheadAgent> getReadAheadAgents() {
        return readAheadAgentLRUCache.cache.keySet();
    }

    @Override
    public void onCacheEvict(long streamId, long startOffset, long endOffset, int size) {
        NavigableMap<Long, ReadAheadAgent> agentMap = readAheadAgentMap.get(streamId);
        if (agentMap != null) {
            synchronized (agentMap) {
                Long floor = agentMap.floorKey(startOffset);
                if (floor == null) {
                    floor = agentMap.firstKey();
                }
                Long ceil = agentMap.ceilingKey(endOffset);
                if (ceil == null) {
                    ceil = agentMap.lastKey();
                }
                NavigableMap<Long, ReadAheadAgent> subMap = agentMap.subMap(floor, true, ceil, Objects.equals(ceil, agentMap.lastKey()));
                for (Map.Entry<Long, ReadAheadAgent> entry : subMap.entrySet()) {
                    ReadAheadAgent agent = entry.getValue();
                    agent.evict(startOffset, endOffset);
                }
            }
        }
    }
}
