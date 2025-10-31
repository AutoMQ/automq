package com.automq.log.uploader.selector.runtime;

import com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider;

/**
 * Selector that follows the Kafka Connect distributed herder leader election state.
 */
public class ConnectLeaderSelectorProvider extends AbstractRuntimeLeaderSelectorProvider implements LogUploaderNodeSelectorProvider {
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
