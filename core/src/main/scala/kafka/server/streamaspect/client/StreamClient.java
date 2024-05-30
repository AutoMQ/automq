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

package kafka.server.streamaspect.client;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.S3Operator;

public class StreamClient {
    private final Config streamConfig;
    private final S3Operator s3Operator;

    public StreamClient(Config streamConfig, S3Operator s3Operator) {
        this.streamConfig = streamConfig;
        this.s3Operator = s3Operator;
    }

    public Config streamConfig() {
        return streamConfig;
    }

    public S3Operator s3Operator() {
        return s3Operator;
    }
}
