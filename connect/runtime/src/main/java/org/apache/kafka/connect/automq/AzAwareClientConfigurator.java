package org.apache.kafka.connect.automq;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (azOpt.isEmpty()) {
            return;
        }

        String az = azOpt.get();
        if (!props.containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
            return;
        }
        Object currentId = props.get(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (!(currentId instanceof String currentIdStr)) {
            return;
        }
        if (!currentIdStr.equals(defaultClientId)) {
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
        LOGGER.debug("Applied AZ-aware client.id for role {} -> {}", roleDescriptor, automqClientId);

        if (family == ClientFamily.CONSUMER) {
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
