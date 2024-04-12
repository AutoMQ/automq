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

package org.apache.kafka.tools.automq;

import org.apache.kafka.tools.automq.perf.PerfConfig;

public class PerfCommand implements AutoCloseable {

    public static void main(String[] args) throws Exception {
        PerfConfig config = new PerfConfig(args);
        try (PerfCommand command = new PerfCommand(config)) {
            command.run(config);
        }
    }

    private PerfCommand(PerfConfig config) {
        // TODO
    }

    private void run(PerfConfig config) {
        // TODO
    }

    @Override
    public void close() {
        // TODO
    }
}
