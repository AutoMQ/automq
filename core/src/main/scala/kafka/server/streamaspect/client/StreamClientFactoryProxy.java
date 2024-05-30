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

import java.lang.reflect.Method;
import org.apache.kafka.controller.stream.StreamClient;

public class StreamClientFactoryProxy {
    private static final String PROTOCOL_SEPARATOR = ":";
    private static final String FACTORY_CLASS_FORMAT = "kafka.server.streamaspect.client.%s.StreamClientFactory";

    public static StreamClient get(Context context) {
        String endpoint = context.kafkaConfig.elasticStreamEndpoint();
        String protocol = endpoint.split(PROTOCOL_SEPARATOR)[0];
        String factoryClassName = String.format(FACTORY_CLASS_FORMAT, protocol);
        try {
            Class<?> factoryClass = Class.forName(factoryClassName);
            Method method = factoryClass.getMethod("get", Context.class);
            return (StreamClient) method.invoke(null, context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create StreamClient", e);
        }
    }
}
