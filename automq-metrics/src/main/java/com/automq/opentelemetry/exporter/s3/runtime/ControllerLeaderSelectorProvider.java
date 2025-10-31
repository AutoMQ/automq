package com.automq.opentelemetry.exporter.s3.runtime;

import com.automq.opentelemetry.exporter.s3.UploaderNodeSelectorProvider;

/**
 * Supplies leadership based on the KRaft controller's active status.
 */
public class ControllerLeaderSelectorProvider extends AbstractRuntimeLeaderSelectorProvider implements UploaderNodeSelectorProvider {
    public static final String TYPE = "controller";
    static final String REGISTRY_KEY = "controller";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected String registryKey() {
        return REGISTRY_KEY;
    }
}
