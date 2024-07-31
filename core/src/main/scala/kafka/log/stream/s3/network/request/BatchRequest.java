/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.network.request;

import org.apache.kafka.common.requests.AbstractRequest.Builder;

public abstract class BatchRequest implements WrapRequest {
    public abstract Builder addSubRequest(Builder builder);

    public Object batchKey() {
        return apiKey();
    }
}
