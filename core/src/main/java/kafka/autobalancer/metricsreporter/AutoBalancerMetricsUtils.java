/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.metricsreporter;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 */
public final class AutoBalancerMetricsUtils {

    public static final long ADMIN_CLIENT_CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long DEFAULT_RETRY_BACKOFF_SCALE_MS = TimeUnit.SECONDS.toMillis(5);
    private static final int DEFAULT_RETRY_BACKOFF_BASE = 2;

    private AutoBalancerMetricsUtils() {

    }

    private static void closeClientWithTimeout(Runnable clientCloseTask, long timeoutMs) {
        Thread t = new Thread(clientCloseTask);
        t.setDaemon(true);
        t.start();
        try {
            t.join(timeoutMs);
        } catch (InterruptedException e) {
            // let it go
        }
        if (t.isAlive()) {
            t.interrupt();
        }
    }

    /**
     * Create an instance of AdminClient using the given configurations.
     *
     * @param adminClientConfigs Configurations used for the AdminClient.
     * @return A new instance of AdminClient.
     */
    public static AdminClient createAdminClient(Properties adminClientConfigs) {
        return AdminClient.create(adminClientConfigs);
    }

    /**
     * Close the given AdminClient with the default timeout of {@link #ADMIN_CLIENT_CLOSE_TIMEOUT_MS}.
     *
     * @param adminClient AdminClient to be closed
     */
    public static void closeAdminClientWithTimeout(AdminClient adminClient) {
        closeAdminClientWithTimeout(adminClient, ADMIN_CLIENT_CLOSE_TIMEOUT_MS);
    }

    /**
     * Close the given AdminClient with the given timeout.
     *
     * @param adminClient AdminClient to be closed.
     * @param timeoutMs   the timeout.
     */
    public static void closeAdminClientWithTimeout(AdminClient adminClient, long timeoutMs) {
        closeClientWithTimeout(() -> {
            try {
                ((AutoCloseable) adminClient).close();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to close the Admin Client.", e);
            }
        }, timeoutMs);
    }

    /**
     * Create a config altering operation if config's current value does not equal to target value.
     *
     * @param configsToAlter Set of config altering operations to be applied.
     * @param configsToSet   Configs to set.
     * @param currentConfig  Current value of the config.
     */
    public static void maybeUpdateConfig(Set<AlterConfigOp> configsToAlter,
                                         Map<String, String> configsToSet,
                                         Config currentConfig) {
        for (Map.Entry<String, String> entry : configsToSet.entrySet()) {
            String configName = entry.getKey();
            String targetConfigValue = entry.getValue();
            if (currentConfig.get(configName) == null || !currentConfig.get(configName).value().equals(targetConfigValue)) {
                configsToAlter.add(new AlterConfigOp(new ConfigEntry(configName, targetConfigValue), AlterConfigOp.OpType.SET));
            }
        }
    }

    /**
     * Retries the {@code Supplier<Boolean>} function while it returns {@code true} and for the specified max number of attempts.
     * The delay between each attempt is computed as: delay = scaleMs * base ^ attempt
     *
     * @param function    the code to call and retry if needed
     * @param scaleMs     the scale for computing the delay
     * @param base        the base for computing the delay
     * @param maxAttempts the max number of attempts on calling the function
     * @return {@code false} if the function requires a retry, but it cannot be retried, because the max attempts have been exceeded.
     * {@code true} if the function stopped requiring a retry before exceeding the max attempts.
     */
    public static boolean retry(Supplier<Boolean> function, long scaleMs, int base, int maxAttempts) {
        if (maxAttempts > 0) {
            int attempts = 0;
            long timeToSleep = scaleMs;
            boolean retry;
            do {
                retry = function.get();
                if (retry) {
                    try {
                        if (++attempts == maxAttempts) {
                            return false;
                        }
                        timeToSleep *= base;
                        Thread.sleep(timeToSleep);
                    } catch (InterruptedException ignored) {

                    }
                }
            } while (retry);
        } else {
            throw new ConfigException("Max attempts has to be greater than zero.");
        }
        return true;
    }

    /**
     * Retries the {@code Supplier<Boolean>} function while it returns {@code true} and for the specified max number of attempts.
     * It uses {@code DEFAULT_RETRY_BACKOFF_SCALE_MS} and {@code DEFAULT_RETRY_BACKOFF_BASE} for scale and base to compute the delay.
     *
     * @param function    the code to call and retry if needed
     * @param maxAttempts the max number of attempts on calling the function
     * @return {@code false} if the function requires a retry, but it cannot be retried, because the max attempts have been exceeded.
     * {@code true} if the function stopped requiring a retry before exceeding the max attempts.
     */
    public static boolean retry(Supplier<Boolean> function, int maxAttempts) {
        return retry(function, DEFAULT_RETRY_BACKOFF_SCALE_MS, DEFAULT_RETRY_BACKOFF_BASE, maxAttempts);
    }
}
