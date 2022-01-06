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

package org.apache.kafka.metalog;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metalog.LocalLogManager.SharedLogData;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class LocalLogManagerTestEnv implements AutoCloseable {
    private static final Logger log =
        LoggerFactory.getLogger(LocalLogManagerTestEnv.class);

    private final String clusterId;

    /**
     * The first error we encountered during this test, or the empty string if we have
     * not encountered any.
     */
    final AtomicReference<String> firstError = new AtomicReference<>(null);

    /**
     * The test directory, which we will delete once the test is over.
     */
    private final File dir;

    /**
     * The shared data for our LocalLogManager instances.
     */
    private final SharedLogData shared;

    /**
     * A list of log managers.
     */
    private final List<LocalLogManager> logManagers;

    public static LocalLogManagerTestEnv createWithMockListeners(
        int numManagers,
        Optional<RawSnapshotReader> snapshot
    ) throws Exception {
        LocalLogManagerTestEnv testEnv = new LocalLogManagerTestEnv(numManagers, snapshot);
        try {
            for (LocalLogManager logManager : testEnv.logManagers) {
                logManager.register(new MockMetaLogManagerListener(logManager.nodeId().getAsInt()));
            }
        } catch (Exception e) {
            testEnv.close();
            throw e;
        }
        return testEnv;
    }

    public LocalLogManagerTestEnv(int numManagers, Optional<RawSnapshotReader> snapshot) throws Exception {
        clusterId = Uuid.randomUuid().toString();
        dir = TestUtils.tempDirectory();
        shared = new SharedLogData(snapshot);
        List<LocalLogManager> newLogManagers = new ArrayList<>(numManagers);
        try {
            for (int nodeId = 0; nodeId < numManagers; nodeId++) {
                newLogManagers.add(new LocalLogManager(
                    new LogContext(String.format("[LocalLogManager %d] ", nodeId)),
                    nodeId,
                    shared,
                    String.format("LocalLogManager-%d_", nodeId)));
            }
            for (LocalLogManager logManager : newLogManagers) {
                logManager.initialize();
            }
        } catch (Throwable t) {
            for (LocalLogManager logManager : newLogManagers) {
                logManager.close();
            }
            throw t;
        }
        this.logManagers = newLogManagers;
    }

    public String clusterId() {
        return clusterId;
    }

    AtomicReference<String> firstError() {
        return firstError;
    }

    File dir() {
        return dir;
    }

    LeaderAndEpoch waitForLeader() throws InterruptedException {
        AtomicReference<LeaderAndEpoch> value = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(20000, 3, () -> {
            LeaderAndEpoch result = null;
            for (LocalLogManager logManager : logManagers) {
                LeaderAndEpoch leader = logManager.leaderAndEpoch();
                int nodeId = logManager.nodeId().getAsInt();
                if (leader.isLeader(nodeId)) {
                    if (result != null) {
                        throw new RuntimeException("node " + nodeId +
                            " thinks it's the leader, but so does " + result.leaderId());
                    }
                    result = leader;
                }
            }
            if (result == null) {
                throw new RuntimeException("No leader found.");
            }
            value.set(result);
        });
        return value.get();
    }

    public List<LocalLogManager> logManagers() {
        return logManagers;
    }

    public RawSnapshotReader waitForSnapshot(long committedOffset) throws InterruptedException {
        return shared.waitForSnapshot(committedOffset);
    }

    public RawSnapshotReader waitForLatestSnapshot() throws InterruptedException {
        return shared.waitForLatestSnapshot();
    }

    public long appendedBytes() {
        return shared.appendedBytes();
    }

    public LeaderAndEpoch leaderAndEpoch() {
        return shared.leaderAndEpoch();
    }

    @Override
    public void close() throws InterruptedException {
        try {
            for (LocalLogManager logManager : logManagers) {
                logManager.beginShutdown();
            }
            for (LocalLogManager logManager : logManagers) {
                logManager.close();
            }
            Utils.delete(dir);
        } catch (IOException e) {
            log.error("Error deleting {}", dir.getAbsolutePath(), e);
        }
    }
}
