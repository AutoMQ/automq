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

package kafka.controller.streamaspect.client;

import org.apache.kafka.controller.stream.StreamClient;

import java.lang.reflect.Method;

public class StreamClientFactoryProxy {
    private static final String PROTOCOL_SEPARATOR = ":";
    private static final String FACTORY_CLASS_FORMAT = "kafka.controller.streamaspect.client.%s.StreamClientFactory";

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
