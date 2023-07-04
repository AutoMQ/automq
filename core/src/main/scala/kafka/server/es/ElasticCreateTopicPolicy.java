package kafka.server.es;

import java.util.Map;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

/**
 * <p>A policy on create topic requests.
 */
public class ElasticCreateTopicPolicy implements CreateTopicPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.replicationFactor() != null && requestMetadata.replicationFactor() != 1) {
            throw new PolicyViolationException("Replication factor must be 1");
        }
        if (requestMetadata.replicasAssignments() != null) {
            requestMetadata.replicasAssignments().forEach((key, value) -> {
                if (value.size() != 1) {
                    throw new PolicyViolationException("Partition " + key + " can only have exactly one replica, but got " + value.size() + " replicas");
                }
            });
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
