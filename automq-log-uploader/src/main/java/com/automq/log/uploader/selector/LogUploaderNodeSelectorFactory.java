package com.automq.log.uploader.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory that resolves node selectors from configuration.
 */
public final class LogUploaderNodeSelectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUploaderNodeSelectorFactory.class);
    private static final Map<String, LogUploaderNodeSelectorProvider> PROVIDERS = new HashMap<>();

    static {
        ServiceLoader<LogUploaderNodeSelectorProvider> loader = ServiceLoader.load(LogUploaderNodeSelectorProvider.class);
        for (LogUploaderNodeSelectorProvider provider : loader) {
            String type = provider.getType();
            if (type != null) {
                PROVIDERS.put(type.toLowerCase(Locale.ROOT), provider);
                LOGGER.info("Loaded LogUploaderNodeSelectorProvider for type {}", type);
            }
        }
    }

    private LogUploaderNodeSelectorFactory() {
    }

    public static LogUploaderNodeSelector createSelector(String typeString,
                                                         String clusterId,
                                                         int nodeId,
                                                         Map<String, String> config) {
        LogUploaderNodeSelectorType type = LogUploaderNodeSelectorType.fromString(typeString);
        switch (type) {
            case STATIC:
                boolean isPrimary = Boolean.parseBoolean(config.getOrDefault("isPrimaryUploader", "false"));
                return LogUploaderNodeSelectors.staticSelector(isPrimary);
            case NODE_ID:
                int primaryNodeId = Integer.parseInt(config.getOrDefault("primaryNodeId", "0"));
                return LogUploaderNodeSelectors.nodeIdSelector(nodeId, primaryNodeId);
            case FILE:
                String leaderFile = config.getOrDefault("leaderFile", "/tmp/log-uploader-leader");
                long timeoutMs = Long.parseLong(config.getOrDefault("leaderTimeoutMs", "60000"));
                return LogUploaderNodeSelectors.fileLeaderElectionSelector(leaderFile, nodeId, timeoutMs);
            case CUSTOM:
                LogUploaderNodeSelectorProvider provider = PROVIDERS.get(typeString.toLowerCase(Locale.ROOT));
                if (provider != null) {
                    try {
                        return provider.createSelector(clusterId, nodeId, config);
                    } catch (Exception e) {
                        LOGGER.error("Failed to create selector of type {}", typeString, e);
                    }
                }
                LOGGER.warn("Unsupported log uploader selector type {}, falling back to static=false", typeString);
                return LogUploaderNodeSelector.staticSelector(false);
            default:
                return LogUploaderNodeSelector.staticSelector(false);
        }
    }

    public static boolean isSupported(String typeString) {
        if (typeString == null) {
            return true;
        }
        LogUploaderNodeSelectorType type = LogUploaderNodeSelectorType.fromString(typeString);
        if (type != LogUploaderNodeSelectorType.CUSTOM) {
            return true;
        }
        return PROVIDERS.containsKey(typeString.toLowerCase(Locale.ROOT));
    }
}
