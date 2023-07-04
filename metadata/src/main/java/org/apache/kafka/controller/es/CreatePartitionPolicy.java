package org.apache.kafka.controller.es;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.PolicyViolationException;

/**
 * <p>An interface for enforcing a policy on create partition requests.
 */
public interface CreatePartitionPolicy extends Configurable, AutoCloseable {
    /**
     * Class containing the create request parameters.
     */
    class RequestMetadata {
        private final List<Integer> replicas;
        private final List<Integer> isrs;

        /**
         * Create an instance of this class with the provided parameters.
         * <p>
         * This constructor is public to make testing of <code>CreatePartitionPolicy</code> implementations easier.
         *
         * @param replicas replica assignments of the partition
         * @param isrs     in-sync replica assignments of the partition
         */
        public RequestMetadata(List<Integer> replicas, List<Integer> isrs) {
            this.replicas = replicas;
            this.isrs = isrs;
        }

        public List<Integer> replicas() {
            return replicas;
        }

        public List<Integer> isrs() {
            return isrs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isrs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CreatePartitionPolicy.RequestMetadata other = (CreatePartitionPolicy.RequestMetadata) o;
            return Objects.equals(replicas, other.replicas) &&
                Objects.equals(isrs, other.isrs);
        }

        @Override
        public String toString() {
            return "CreatePartitionPolicy.RequestMetadata(replicas=" + replicas +
                ", isrs=" + isrs + ")";
        }
    }

    /**
     * Validate the request parameters and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the parameters for the provided partition do not satisfy this policy.
     * <p>
     *
     * @param requestMetadata the create partition request parameters for the provided partition.
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validate(CreatePartitionPolicy.RequestMetadata requestMetadata) throws PolicyViolationException;
}
