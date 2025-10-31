package com.automq.opentelemetry.exporter.s3.runtime;

import com.automq.opentelemetry.exporter.s3.UploaderNodeSelectorProvider;

/**
 * Uses the Kafka Connect distributed herder leadership to decide whether metrics should be uploaded.
 */
public class ConnectLeaderSelectorProvider extends AbstractRuntimeLeaderSelectorProvider implements UploaderNodeSelectorProvider {
    public static final String TYPE = "connect-leader";
    static final String REGISTRY_KEY = "connect-leader";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected String registryKey() {
        return REGISTRY_KEY;
    }
}
