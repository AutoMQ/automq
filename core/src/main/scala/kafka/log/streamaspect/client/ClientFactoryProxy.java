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

package kafka.log.streamaspect.client;

import com.automq.stream.api.Client;

import java.lang.reflect.Method;

public class ClientFactoryProxy {
    private static final String PROTOCOL_SEPARATOR = ":";
    private static final String FACTORY_CLASS_FORMAT = "kafka.log.streamaspect.client.%s.ClientFactory";

    public static Client get(Context context) {
        String endpoint = context.config.elasticStreamEndpoint();
        String protocol = endpoint.split(PROTOCOL_SEPARATOR)[0];
        String className = String.format(FACTORY_CLASS_FORMAT, protocol);
        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getDeclaredMethod("get", Context.class);
            return (Client) method.invoke(null, context);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
