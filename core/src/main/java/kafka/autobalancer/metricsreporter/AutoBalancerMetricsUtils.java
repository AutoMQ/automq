/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Some portion of this file Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.autobalancer.metricsreporter;

import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.
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
     * Parse AdminClient configs based on the given {@link AutoBalancerMetricsReporterConfig configs}.
     *
     * @param adminClientConfigs Configs that will be return with SSL configs.
     * @param configs            Configs to be used for parsing AdminClient SSL configs.
     * @return AdminClient configs.
     */
    public static Properties addSslConfigs(Properties adminClientConfigs, AutoBalancerMetricsReporterConfig configs) {
        // Add security protocol (if specified).
        try {
            String securityProtocol = configs.getString(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
            adminClientConfigs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            setStringConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_MECHANISM);
            setPasswordConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_JAAS_CONFIG);

            // Configure SSL configs (if security protocol is SSL or SASL_SSL)
            if (securityProtocol.equals(SecurityProtocol.SSL.name) || securityProtocol.equals(SecurityProtocol.SASL_SSL.name)) {
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
                setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
                setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
                setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
                setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
            }
        } catch (ConfigException ce) {
            // let it go.
        }

        return adminClientConfigs;
    }

    private static void setPasswordConfigIfExists(AutoBalancerMetricsReporterConfig configs, Properties props, String name) {
        try {
            props.put(name, configs.getPassword(name));
        } catch (ConfigException ce) {
            // let it go.
        }
    }

    private static void setStringConfigIfExists(AutoBalancerMetricsReporterConfig configs, Properties props, String name) {
        try {
            props.put(name, configs.getString(name));
        } catch (ConfigException ce) {
            // let it go.
        }
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
