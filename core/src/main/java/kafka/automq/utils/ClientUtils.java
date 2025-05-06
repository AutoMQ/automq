package kafka.automq.utils;

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.network.ListenerName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static scala.jdk.javaapi.CollectionConverters.asJava;

public class ClientUtils {
    public static Properties clusterClientBaseConfig(KafkaConfig kafkaConfig) {
        ListenerName listenerName = kafkaConfig.interBrokerListenerName();
        List<EndPoint> endpoints = asJava(kafkaConfig.effectiveAdvertisedBrokerListeners());
        Optional<EndPoint> endpointOpt = endpoints.stream().filter(e -> listenerName.equals(e.listenerName())).findFirst();
        if (endpointOpt.isEmpty()) {
            throw new IllegalArgumentException("Cannot find " + listenerName + " in endpoints " + endpoints);
        }
        EndPoint endpoint = endpointOpt.get();
        Map<String, ?> securityConfig = kafkaConfig.originalsWithPrefix(listenerName.saslMechanismConfigPrefix(kafkaConfig.saslMechanismInterBrokerProtocol()));
        Properties clientConfig = new Properties();
        clientConfig.putAll(securityConfig);
        clientConfig.put("security.protocol", kafkaConfig.interBrokerSecurityProtocol().toString());
        clientConfig.put("sasl.mechanism", kafkaConfig.saslMechanismInterBrokerProtocol());
        clientConfig.put("bootstrap.servers", String.format("%s:%d", endpoint.host(), endpoint.port()));
        return clientConfig;
    }

}
