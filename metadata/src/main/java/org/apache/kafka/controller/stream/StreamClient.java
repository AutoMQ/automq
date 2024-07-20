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

package org.apache.kafka.controller.stream;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.ObjectStorage;

public class StreamClient {
    private Config streamConfig;
    private ObjectStorage objectStorage;

    public Config streamConfig() {
        return streamConfig;
    }

    public ObjectStorage objectStorage() {
        return objectStorage;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final StreamClient streamClient = new StreamClient();

        public Builder streamConfig(Config streamConfig) {
            streamClient.streamConfig = streamConfig;
            return this;
        }

        public Builder objectStorage(ObjectStorage objectStorage) {
            streamClient.objectStorage = objectStorage;
            return this;
        }

        public StreamClient build() {
            return streamClient;
        }
    }
}
