package org.apache.kafka.connect.automq;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public final class AzAwareClientConfigurator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzAwareClientConfigurator.class);

    private AzAwareClientConfigurator() {
    }

    public enum ClientFamily {
        PRODUCER,
        CONSUMER,
        ADMIN
    }

    public static void maybeApplyAz(Map<String, Object> props, String defaultClientId, ClientFamily family, String roleDescriptor) {
        Optional<String> azOpt = AzMetadataProviderHolder.provider().availabilityZoneId();
        LOGGER.info("AZ-aware client.id configuration for role {}: resolved availability zone id '{}'",
            roleDescriptor, azOpt.orElse("unknown"));
        if (azOpt.isEmpty()) {
            LOGGER.info("Skipping AZ-aware client.id configuration for role {} as no availability zone id is available",
                roleDescriptor);
            return;
        }

        String az = azOpt.get();
        if (!props.containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
            LOGGER.info("No client.id configured for role {}; skipping AZ-aware configuration", roleDescriptor);
            return;
        }
        Object currentId = props.get(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (!(currentId instanceof String currentIdStr)) {
            LOGGER.warn("client.id for role {} is not a string ({}); skipping AZ-aware configuration",
                roleDescriptor, currentId.getClass().getName());
            return;
        }
        if (!currentIdStr.equals(defaultClientId)) {
            LOGGER.warn("client.id for role {} is not the same as the default client", roleDescriptor);
            // User has overridden the client.id; respect it.
            return;
        }

        String encodedClientId = URLEncoder.encode(defaultClientId, StandardCharsets.UTF_8);
        String encodedAz = URLEncoder.encode(az, StandardCharsets.UTF_8);
        String type = switch (family) {
            case PRODUCER -> "producer";
            case CONSUMER -> "consumer";
            case ADMIN -> "admin";
        };
        String encodedRole = URLEncoder.encode(roleDescriptor.toLowerCase(Locale.ROOT), StandardCharsets.UTF_8);
        String automqClientId = "automq_type=" + type
            + "&automq_role=" + encodedRole
            + "&automq_client_id=" + encodedClientId
            + "&automq_az=" + encodedAz;
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, automqClientId);
        LOGGER.info("Applied AZ-aware client.id for role {} -> {}", roleDescriptor, automqClientId);

        if (family == ClientFamily.CONSUMER) {
            LOGGER.info("Applying client.rack configuration for consumer role {} -> {}", roleDescriptor, az);
            Object rackValue = props.get(ConsumerConfig.CLIENT_RACK_CONFIG);
            if (rackValue == null || String.valueOf(rackValue).isBlank()) {
                props.put(ConsumerConfig.CLIENT_RACK_CONFIG, az);
            }
        }
    }

    public static void maybeApplyProducerAz(Map<String, Object> props, String defaultClientId, String roleDescriptor) {
        maybeApplyAz(props, defaultClientId, ClientFamily.PRODUCER, roleDescriptor);
    }

    public static void maybeApplyConsumerAz(Map<String, Object> props, String defaultClientId, String roleDescriptor) {
        maybeApplyAz(props, defaultClientId, ClientFamily.CONSUMER, roleDescriptor);
    }

    public static void maybeApplyAdminAz(Map<String, Object> props, String defaultClientId, String roleDescriptor) {
        maybeApplyAz(props, defaultClientId, ClientFamily.ADMIN, roleDescriptor);
    }
}
