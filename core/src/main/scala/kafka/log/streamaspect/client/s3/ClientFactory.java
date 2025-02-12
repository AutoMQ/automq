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

package kafka.log.streamaspect.client.s3;

import kafka.log.stream.s3.ConfigUtils;
import kafka.log.stream.s3.DefaultS3Client;
import kafka.log.streamaspect.ClientWrapper;
import kafka.log.streamaspect.client.ClientFactoryProxy;
import kafka.log.streamaspect.client.Context;

import com.automq.stream.api.Client;
import com.automq.stream.s3.Config;

public class ClientFactory {

    /**
     * This method will be called by {@link ClientFactoryProxy}
     */
    public static Client get(Context context) {
        Config config = ConfigUtils.to(context.config);
        config.nodeEpoch(System.currentTimeMillis());
        config.version(() -> context.brokerServer.metadataCache().autoMQVersion().s3streamVersion());

        DefaultS3Client client = new DefaultS3Client(context.brokerServer, config);
        return new ClientWrapper(client);
    }
}
