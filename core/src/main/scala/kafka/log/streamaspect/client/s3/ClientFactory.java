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

package kafka.log.streamaspect.client.s3;

import com.automq.stream.api.Client;
import kafka.log.stream.s3.DefaultS3Client;
import kafka.log.streamaspect.AlwaysSuccessClient;
import kafka.log.streamaspect.client.Context;

public class ClientFactory {
    public static Client get(Context context) {
        DefaultS3Client client = new DefaultS3Client(context.brokerServer, context.config);
        return new AlwaysSuccessClient(client);
    }
}
