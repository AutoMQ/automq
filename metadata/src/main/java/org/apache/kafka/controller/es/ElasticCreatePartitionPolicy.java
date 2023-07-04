package org.apache.kafka.controller.es;

import java.util.Map;
import org.apache.kafka.common.errors.PolicyViolationException;

/**
 * <p>A policy on create partition requests.
 */
public class ElasticCreatePartitionPolicy implements CreatePartitionPolicy{
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.replicas() == null || requestMetadata.replicas().size() != 1) {
            throw new PolicyViolationException("There should be exactly one replica, but got " + requestMetadata.replicas().size() + " replicas");
        }
        if (requestMetadata.isrs() == null || requestMetadata.isrs().size() != 1) {
            throw new PolicyViolationException("There should be exactly one in-sync replica, but got " + requestMetadata.isrs().size() + " in-sync replicas");
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
