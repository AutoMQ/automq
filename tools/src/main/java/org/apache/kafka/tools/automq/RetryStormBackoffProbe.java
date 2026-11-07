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

package org.apache.kafka.tools.automq;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.tools.TerseException;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * System-test probe that repeatedly describes one topic through a single AdminClient connection.
 *
 * <p>The probe is intentionally small and prints one timing line per request so ducktape tests can
 * validate retry storm delayed-response behavior without compiling ad-hoc Java on remote nodes.
 */
public class RetryStormBackoffProbe {

    /**
     * Runs the probe and exits with a process status that reflects argument or client failures.
     *
     * @param args bootstrap servers, topic name, and iteration count
     */
    public static void main(String[] args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String[] args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    private static void execute(String[] args) throws Exception {
        if (args.length != 3) {
            throw new TerseException("USAGE: " + RetryStormBackoffProbe.class.getName()
                + " bootstrap_servers topic iterations");
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        int iterations = Integer.parseInt(args[2]);
        if (iterations <= 0) {
            throw new TerseException("iterations must be positive");
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "retry-storm-e2e-probe");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

        try (Admin admin = Admin.create(props)) {
            for (int i = 1; i <= iterations; i++) {
                long start = System.nanoTime();
                String result = "ok";
                try {
                    admin.describeTopics(List.of(topic)).allTopicNames().get();
                } catch (ExecutionException e) {
                    result = e.getCause().getClass().getSimpleName();
                }
                long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
                System.out.println(i + " elapsedMs=" + elapsedMs + " result=" + result);
            }
        }
    }
}
