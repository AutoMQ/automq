/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect.client;

import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;

public class Context {
    // TODO: remove sync test mode, test should keep the same logic as production code.
    private static boolean testMode = false;
    public KafkaConfig config;
    public BrokerServer brokerServer;

    public static void enableTestMode() {
        testMode = true;
    }

    public static boolean isTestMode() {
        return testMode;
    }
}
