package com.automq.log.uploader.selector.runtime;

import com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider;

/**
 * Selector that delegates leadership to the active controller status exposed by the core runtime.
 */
public class ControllerLeaderSelectorProvider extends AbstractRuntimeLeaderSelectorProvider implements LogUploaderNodeSelectorProvider {
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
