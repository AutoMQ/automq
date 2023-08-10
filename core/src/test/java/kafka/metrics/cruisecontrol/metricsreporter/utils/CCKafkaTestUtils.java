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
/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter.utils;

import org.apache.directory.api.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


public final class CCKafkaTestUtils {
    private static final AtomicBoolean SHUTDOWN_HOOK_INSTALLED = new AtomicBoolean(false);
    private static final Thread SHUTDOWN_HOOK;
    private static final List<File> FILES_TO_CLEAN_UP = Collections.synchronizedList(new ArrayList<>());

    static {
        SHUTDOWN_HOOK = new Thread(() -> {
            Exception firstIssue = null;
            for (File toCleanUp : FILES_TO_CLEAN_UP) {
                if (!toCleanUp.exists()) {
                    continue;
                }
                try {
                    FileUtils.forceDelete(toCleanUp);
                } catch (IOException issue) {
                    if (firstIssue == null) {
                        firstIssue = issue;
                    } else {
                        firstIssue.addSuppressed(issue);
                    }
                }
            }
            if (firstIssue != null) {
                System.err.println("unable to delete one or more files");
                firstIssue.printStackTrace(System.err);
                throw new IllegalStateException(firstIssue);
            }
        }, "CCKafkaTestUtils cleanup hook");
        SHUTDOWN_HOOK.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("thread " + t.getName() + " died to uncaught exception");
            e.printStackTrace(System.err);
        });
    }

    private CCKafkaTestUtils() {
        //utility class
    }

    /**
     * Cleanup the given file.
     *
     * @param toCleanUp File to cleanup.
     * @return File to be cleaned up.
     */
    public static File cleanup(File toCleanUp) {
        if (SHUTDOWN_HOOK_INSTALLED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
        }
        FILES_TO_CLEAN_UP.add(toCleanUp);
        return toCleanUp;
    }

    /**
     * Find a local port.
     *
     * @return A local port to use.
     */
    public static int[] findLocalPorts(int portNum) {
        int[] ports = new int[portNum];
        List<ServerSocket> sockets = new ArrayList<>();
        for (int i = 0; i < portNum; i++) {
            int port = -1;
            while (port < 0) {
                try {
                    ServerSocket socket = new ServerSocket(0);
                    socket.setReuseAddress(true);
                    port = socket.getLocalPort();
                    ports[i] = port;
                    sockets.add(socket);
                } catch (IOException ie) {
                    // let it go.
                }
            }
        }
        for (ServerSocket socket : sockets) {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore IOException on close()
            }
        }
        return ports;
    }

    /**
     * A functional interface for a task to run.
     */
    @FunctionalInterface
    public interface Task {
        void run() throws Exception;
    }
}
