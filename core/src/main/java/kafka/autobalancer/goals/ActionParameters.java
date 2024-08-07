package kafka.autobalancer.goals;

import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ActionParameters {
    ClusterModelSnapshot cluster;
    TopicPartitionReplicaUpdater.TopicPartitionReplica replica;
    BrokerUpdater.Broker srcBroker;
    List<BrokerUpdater.Broker> candidates;
    Collection<Goal> goalsByPriority;
    Collection<Goal> optimizedGoals;
    Map<String, Set<String>> goalsByGroup;

    public ActionParameters(ClusterModelSnapshot cluster,
                            TopicPartitionReplicaUpdater.TopicPartitionReplica replica,
                            BrokerUpdater.Broker srcBroker,
                            List<BrokerUpdater.Broker> candidates,
                            Collection<Goal> goalsByPriority,
                            Collection<Goal> optimizedGoals,
                            Map<String, Set<String>> goalsByGroup) {
        this.cluster = cluster;
        this.replica = replica;
        this.srcBroker = srcBroker;
        this.candidates = candidates;
        this.goalsByPriority = goalsByPriority;
        this.optimizedGoals = optimizedGoals;
        this.goalsByGroup = goalsByGroup;
    }
}
