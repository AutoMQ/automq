package com.automq.log.uploader.selector.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;

/**
 * Holds runtime supplied leadership callbacks so selector providers can make
 * decisions without creating hard dependencies on hosting processes.
 */
public final class RuntimeLeaderRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeLeaderRegistry.class);
    private static final ConcurrentMap<String, BooleanSupplier> SUPPLIERS = new ConcurrentHashMap<>();

    private RuntimeLeaderRegistry() {
    }

    public static void register(String key, BooleanSupplier supplier) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(supplier, "supplier");
        SUPPLIERS.put(key, supplier);
        LOGGER.info("Registered runtime leader supplier for key {}", key);
    }

    public static void clear(String key) {
        if (key == null) {
            return;
        }
        if (SUPPLIERS.remove(key) != null) {
            LOGGER.info("Cleared runtime leader supplier for key {}", key);
        }
    }

    public static BooleanSupplier supplier(String key) {
        return SUPPLIERS.get(key);
    }
}
