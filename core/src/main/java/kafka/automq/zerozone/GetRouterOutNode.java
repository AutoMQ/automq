package kafka.automq.zerozone;

import kafka.automq.interceptor.ClientIdMetadata;
import org.apache.kafka.common.Node;

public interface GetRouterOutNode {
    Node getRouteOutNode(String topicName, int partition, ClientIdMetadata clientId);
}
