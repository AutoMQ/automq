package org.apache.kafka.connect.automq;

import java.util.Map;
import java.util.Optional;

/**
 * Pluggable provider for availability-zone metadata used to tune Kafka client configurations.
 */
public interface AzMetadataProvider {

    /**
     * Configure the provider with the worker properties. Implementations may cache values extracted from the
     * configuration map. This method is invoked exactly once during worker bootstrap.
     */
    default void configure(Map<String, String> workerProps) {
        // no-op
    }

    /**
     * @return the availability-zone identifier for the current node, if known.
     */
    default Optional<String> availabilityZoneId() {
        return Optional.empty();
    }
}
