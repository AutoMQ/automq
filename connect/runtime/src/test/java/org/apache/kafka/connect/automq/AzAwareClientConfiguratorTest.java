package org.apache.kafka.connect.automq;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.automq.az.AzAwareClientConfigurator;
import org.apache.kafka.connect.automq.az.AzMetadataProvider;
import org.apache.kafka.connect.automq.az.AzMetadataProviderHolder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AzAwareClientConfiguratorTest {

    @AfterEach
    void resetProvider() {
        AzMetadataProviderHolder.setProviderForTest(null);
    }

    @Test
    void shouldDecorateProducerClientId() {
        AzMetadataProviderHolder.setProviderForTest(new FixedAzProvider("us-east-1a"));
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-1");

        AzAwareClientConfigurator.maybeApplyProducerAz(props, "producer-1");

        assertEquals("automq_type=producer&automq_role=producer-1&automq_az=us-east-1a&producer-1",
            props.get(ProducerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    void shouldPreserveCustomClientIdInAzConfig() {
        AzMetadataProviderHolder.setProviderForTest(new FixedAzProvider("us-east-1a"));
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "custom-id");

        AzAwareClientConfigurator.maybeApplyProducerAz(props, "producer-1");

        assertEquals("automq_type=producer&automq_role=producer-1&automq_az=us-east-1a&custom-id",
            props.get(ProducerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    void shouldAssignRackForConsumers() {
        AzMetadataProviderHolder.setProviderForTest(new FixedAzProvider("us-west-2c"));
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-1");

        AzAwareClientConfigurator.maybeApplyConsumerAz(props, "consumer-1");

        assertEquals("us-west-2c", props.get(ConsumerConfig.CLIENT_RACK_CONFIG));
    }

    @Test
    void shouldDecorateAdminClientId() {
        AzMetadataProviderHolder.setProviderForTest(new FixedAzProvider("eu-west-1b"));
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-1");

        AzAwareClientConfigurator.maybeApplyAdminAz(props, "admin-1");

        assertEquals("automq_type=admin&automq_role=admin-1&automq_az=eu-west-1b&admin-1",
            props.get(AdminClientConfig.CLIENT_ID_CONFIG));
    }

    @Test
    void shouldLeaveClientIdWhenAzUnavailable() {
        AzMetadataProviderHolder.setProviderForTest(new AzMetadataProvider() {
            @Override
            public Optional<String> availabilityZoneId() {
                return Optional.empty();
            }
        });
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-1");

        AzAwareClientConfigurator.maybeApplyProducerAz(props, "producer-1");

        assertEquals("producer-1", props.get(ProducerConfig.CLIENT_ID_CONFIG));
        assertFalse(props.containsKey(ConsumerConfig.CLIENT_RACK_CONFIG));
    }

    @Test
    void shouldEncodeSpecialCharactersInClientId() {
        AzMetadataProviderHolder.setProviderForTest(new FixedAzProvider("us-east-1a"));
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-with-spaces & symbols");

        AzAwareClientConfigurator.maybeApplyProducerAz(props, "test-role");

        assertEquals("automq_type=producer&automq_role=test-role&automq_az=us-east-1a&client-with-spaces & symbols",
            props.get(ProducerConfig.CLIENT_ID_CONFIG));
    }

    private static final class FixedAzProvider implements AzMetadataProvider {
        private final String az;

        private FixedAzProvider(String az) {
            this.az = az;
        }

        @Override
        public Optional<String> availabilityZoneId() {
            return Optional.ofNullable(az);
        }
    }
}
