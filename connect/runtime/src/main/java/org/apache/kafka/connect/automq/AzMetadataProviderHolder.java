package org.apache.kafka.connect.automq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;

public final class AzMetadataProviderHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzMetadataProviderHolder.class);
    private static final AzMetadataProvider DEFAULT_PROVIDER = new AzMetadataProvider() { };

    private static volatile AzMetadataProvider provider = DEFAULT_PROVIDER;

    private AzMetadataProviderHolder() {
    }

    public static void initialize(Map<String, String> workerProps) {
        AzMetadataProvider selected = DEFAULT_PROVIDER;
        try {
            ServiceLoader<AzMetadataProvider> loader = ServiceLoader.load(AzMetadataProvider.class);
            for (AzMetadataProvider candidate : loader) {
                try {
                    candidate.configure(workerProps);
                    selected = candidate;
                    LOGGER.info("Loaded AZ metadata provider: {}", candidate.getClass().getName());
                    break;
                } catch (Exception e) {
                    LOGGER.warn("Failed to initialize AZ metadata provider: {}", candidate.getClass().getName(), e);
                }
            }
        } catch (Throwable t) {
            LOGGER.warn("Failed to load AZ metadata providers", t);
        }
        provider = selected;
    }

    public static AzMetadataProvider provider() {
        return provider;
    }

    static void setProviderForTest(AzMetadataProvider newProvider) {
        provider = newProvider != null ? newProvider : DEFAULT_PROVIDER;
    }
}
