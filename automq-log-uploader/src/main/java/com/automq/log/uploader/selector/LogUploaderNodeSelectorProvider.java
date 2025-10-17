package com.automq.log.uploader.selector;

import java.util.Map;

/**
 * Service Provider Interface for custom log uploader node selection strategies.
 */
public interface LogUploaderNodeSelectorProvider {

    /**
     * @return the selector type identifier (case insensitive)
     */
    String getType();

    /**
     * Creates a selector based on the supplied configuration.
     *
     * @param clusterId logical cluster identifier
     * @param nodeId    numeric node identifier
     * @param config    additional selector configuration
     * @return selector instance
     * @throws Exception if creation fails
     */
    LogUploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) throws Exception;
}
