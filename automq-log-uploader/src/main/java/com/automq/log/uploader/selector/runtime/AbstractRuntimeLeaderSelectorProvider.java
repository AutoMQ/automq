package com.automq.log.uploader.selector.runtime;

import com.automq.log.uploader.selector.LogUploaderNodeSelector;
import com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider;
import com.automq.log.uploader.selector.LogUploaderNodeSelectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

abstract class AbstractRuntimeLeaderSelectorProvider implements LogUploaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRuntimeLeaderSelectorProvider.class);

    protected AbstractRuntimeLeaderSelectorProvider() {
    }

    @Override
    public LogUploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) {
        final String key = registryKey();
        final AtomicBoolean missingLogged = new AtomicBoolean(false);
        final AtomicBoolean leaderLogged = new AtomicBoolean(false);

        return LogUploaderNodeSelectors.supplierSelector(() -> {
            BooleanSupplier supplier = RuntimeLeaderRegistry.supplier(key);
            if (supplier == null) {
                if (missingLogged.compareAndSet(false, true)) {
                    LOGGER.warn("No log uploader runtime leader supplier registered for key {} yet; treating node as follower until available.", key);
                }
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from log uploader leadership for key {} because supplier is unavailable.", key);
                }
                return false;
            }

            if (missingLogged.get()) {
                missingLogged.set(false);
                LOGGER.info("Log uploader runtime leader supplier for key {} is now available.", key);
            }

            try {
                boolean leader = supplier.getAsBoolean();
                if (leader) {
                    if (!leaderLogged.getAndSet(true)) {
                        LOGGER.info("Node became log uploader leader for key {}", key);
                    }
                } else {
                    if (leaderLogged.getAndSet(false)) {
                        LOGGER.info("Node stepped down from log uploader leadership for key {}", key);
                    }
                }
                return leader;
            } catch (RuntimeException e) {
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from log uploader leadership for key {} due to supplier exception.", key);
                }
                LOGGER.warn("Log uploader leader supplier {} threw an exception; treating as follower", key, e);
                return false;
            }
        });
    }

    protected abstract String registryKey();
}
