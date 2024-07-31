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

package kafka.log.streamaspect.client.memory;

import kafka.log.streamaspect.AlwaysSuccessClient;
import kafka.log.streamaspect.MemoryClient;
import com.automq.stream.api.Client;
import kafka.log.streamaspect.client.Context;

public class ClientFactory {

    public static Client get(Context context) {
        return new AlwaysSuccessClient(new MemoryClient());
    }

}
