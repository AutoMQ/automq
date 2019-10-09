/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for managing assignments
 * to workers.
 */
public class WorkerCoordinator extends AbstractCoordinator implements Closeable {
    // Currently doesn't support multiple task assignment strategies, so we just fill in a default value
    public static final String DEFAULT_SUBPROTOCOL = "default";

    private final Logger log;
    private final String restUrl;
    private final ConfigBackingStore configStorage;
    private ExtendedAssignment assignmentSnapshot;
    private ClusterConfigState configSnapshot;
    private final WorkerRebalanceListener listener;
    private final ConnectProtocolCompatibility protocolCompatibility;
    private LeaderState leaderState;

    private boolean rejoinRequested;
    private volatile ConnectProtocolCompatibility currentConnectProtocol;
    private final ConnectAssignor eagerAssignor;
    private final ConnectAssignor incrementalAssignor;

    /**
     * Initialize the coordination manager.
     */
    public WorkerCoordinator(GroupRebalanceConfig config,
                             LogContext logContext,
                             ConsumerNetworkClient client,
                             Metrics metrics,
                             String metricGrpPrefix,
                             Time time,
                             String restUrl,
                             ConfigBackingStore configStorage,
                             WorkerRebalanceListener listener,
                             ConnectProtocolCompatibility protocolCompatibility,
                             int maxDelay) {
        super(config,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);
        this.log = logContext.logger(WorkerCoordinator.class);
        this.restUrl = restUrl;
        this.configStorage = configStorage;
        this.assignmentSnapshot = null;
        new WorkerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.listener = listener;
        this.rejoinRequested = false;
        this.protocolCompatibility = protocolCompatibility;
        this.incrementalAssignor = new IncrementalCooperativeAssignor(logContext, time, maxDelay);
        this.eagerAssignor = new EagerAssignor(logContext);
        this.currentConnectProtocol = protocolCompatibility;
    }

    @Override
    public void requestRejoin() {
        rejoinRequested = true;
    }

    @Override
    public String protocolType() {
        return "connect";
    }

    // expose for tests
    @Override
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        return super.ensureCoordinatorReady(timer);
    }

    public void poll(long timeout) {
        // poll for io until the timeout expires
        final long start = time.milliseconds();
        long now = start;
        long remaining;

        do {
            if (coordinatorUnknown()) {
                ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
                now = time.milliseconds();
            }

            if (rejoinNeededOrPending()) {
                ensureActiveGroup();
                now = time.milliseconds();
            }

            pollHeartbeat(now);

            long elapsed = now - start;
            remaining = timeout - elapsed;

            // Note that because the network client is shared with the background heartbeat thread,
            // we do not want to block in poll longer than the time to the next heartbeat.
            long pollTimeout = Math.min(Math.max(0, remaining), timeToNextHeartbeat(now));
            client.poll(time.timer(pollTimeout));

            now = time.milliseconds();
            elapsed = now - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);
    }

    @Override
    public JoinGroupRequestProtocolCollection metadata() {
        configSnapshot = configStorage.snapshot();
        ExtendedWorkerState workerState = new ExtendedWorkerState(restUrl, configSnapshot.offset(), assignmentSnapshot);
        switch (protocolCompatibility) {
            case EAGER:
                return ConnectProtocol.metadataRequest(workerState);
            case COMPATIBLE:
                return IncrementalCooperativeConnectProtocol.metadataRequest(workerState, false);
            case SESSIONED:
                return IncrementalCooperativeConnectProtocol.metadataRequest(workerState, true);
            default:
                throw new IllegalStateException("Unknown Connect protocol compatibility mode " + protocolCompatibility);
        }
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        ExtendedAssignment newAssignment = IncrementalCooperativeConnectProtocol.deserializeAssignment(memberAssignment);
        log.debug("Deserialized new assignment: {}", newAssignment);
        currentConnectProtocol = ConnectProtocolCompatibility.fromProtocol(protocol);
        // At this point we always consider ourselves to be a member of the cluster, even if there was an assignment
        // error (the leader couldn't make the assignment) or we are behind the config and cannot yet work on our assigned
        // tasks. It's the responsibility of the code driving this process to decide how to react (e.g. trying to get
        // up to date, try to rejoin again, leaving the group and backing off, etc.).
        rejoinRequested = false;
        if (currentConnectProtocol != EAGER) {
            if (!newAssignment.revokedConnectors().isEmpty() || !newAssignment.revokedTasks().isEmpty()) {
                listener.onRevoked(newAssignment.leader(), newAssignment.revokedConnectors(), newAssignment.revokedTasks());
            }

            if (assignmentSnapshot != null) {
                assignmentSnapshot.connectors().removeAll(newAssignment.revokedConnectors());
                assignmentSnapshot.tasks().removeAll(newAssignment.revokedTasks());
                log.debug("After revocations snapshot of assignment: {}", assignmentSnapshot);
                newAssignment.connectors().addAll(assignmentSnapshot.connectors());
                newAssignment.tasks().addAll(assignmentSnapshot.tasks());
            }
            log.debug("Augmented new assignment: {}", newAssignment);
        }
        assignmentSnapshot = newAssignment;
        listener.onAssigned(assignmentSnapshot, generation);
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId, String protocol, List<JoinGroupResponseMember> allMemberMetadata) {
        return ConnectProtocolCompatibility.fromProtocol(protocol) == EAGER
               ? eagerAssignor.performAssignment(leaderId, protocol, allMemberMetadata, this)
               : incrementalAssignor.performAssignment(leaderId, protocol, allMemberMetadata, this);
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        log.info("Rebalance started");
        leaderState(null);
        if (currentConnectProtocol == EAGER) {
            log.debug("Revoking previous assignment {}", assignmentSnapshot);
            if (assignmentSnapshot != null && !assignmentSnapshot.failed())
                listener.onRevoked(assignmentSnapshot.leader(), assignmentSnapshot.connectors(), assignmentSnapshot.tasks());
        } else {
            log.debug("Cooperative rebalance triggered. Keeping assignment {} until it's "
                      + "explicitly revoked.", assignmentSnapshot);
        }
    }

    @Override
    protected boolean rejoinNeededOrPending() {
        return super.rejoinNeededOrPending() || (assignmentSnapshot == null || assignmentSnapshot.failed()) || rejoinRequested;
    }

    @Override
    public String memberId() {
        Generation generation = generationIfStable();
        if (generation != null)
            return generation.memberId;
        return JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    private boolean isLeader() {
        return assignmentSnapshot != null && memberId().equals(assignmentSnapshot.leader());
    }

    public String ownerUrl(String connector) {
        if (rejoinNeededOrPending() || !isLeader())
            return null;
        return leaderState().ownerUrl(connector);
    }

    public String ownerUrl(ConnectorTaskId task) {
        if (rejoinNeededOrPending() || !isLeader())
            return null;
        return leaderState().ownerUrl(task);
    }

    /**
     * Get an up-to-date snapshot of the cluster configuration.
     *
     * @return the state of the cluster configuration; the result is not locally cached
     */
    public ClusterConfigState configFreshSnapshot() {
        return configStorage.snapshot();
    }

    /**
     * Get a snapshot of the cluster configuration.
     *
     * @return the state of the cluster configuration
     */
    public ClusterConfigState configSnapshot() {
        return configSnapshot;
    }

    /**
     * Set the state of the cluster configuration to this worker coordinator.
     *
     * @param update the updated state of the cluster configuration
     */
    public void configSnapshot(ClusterConfigState update) {
        configSnapshot = update;
    }

    /**
     * Get the leader state stored in this worker coordinator.
     *
     * @return the leader state
     */
    private LeaderState leaderState() {
        return leaderState;
    }

    /**
     * Store the leader state to this worker coordinator.
     *
     * @param update the updated leader state
     */
    public void leaderState(LeaderState update) {
        leaderState = update;
    }

    /**
     * Get the version of the connect protocol that is currently active in the group of workers.
     *
     * @return the current connect protocol version
     */
    public short currentProtocolVersion() {
        return currentConnectProtocol.protocolVersion();
    }

    private class WorkerCoordinatorMetrics {
        public final String metricGrpName;

        public WorkerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            Measurable numConnectors = new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.connectors().size();
                }
            };

            Measurable numTasks = new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.tasks().size();
                }
            };

            metrics.addMetric(metrics.metricName("assigned-connectors",
                              this.metricGrpName,
                              "The number of connector instances currently assigned to this consumer"), numConnectors);
            metrics.addMetric(metrics.metricName("assigned-tasks",
                              this.metricGrpName,
                              "The number of tasks currently assigned to this consumer"), numTasks);
        }
    }

    public static <K, V> Map<V, K> invertAssignment(Map<K, Collection<V>> assignment) {
        Map<V, K> inverted = new HashMap<>();
        for (Map.Entry<K, Collection<V>> assignmentEntry : assignment.entrySet()) {
            K key = assignmentEntry.getKey();
            for (V value : assignmentEntry.getValue())
                inverted.put(value, key);
        }
        return inverted;
    }

    public static class LeaderState {
        private final Map<String, ExtendedWorkerState> allMembers;
        private final Map<String, String> connectorOwners;
        private final Map<ConnectorTaskId, String> taskOwners;

        public LeaderState(Map<String, ExtendedWorkerState> allMembers,
                           Map<String, Collection<String>> connectorAssignment,
                           Map<String, Collection<ConnectorTaskId>> taskAssignment) {
            this.allMembers = allMembers;
            this.connectorOwners = invertAssignment(connectorAssignment);
            this.taskOwners = invertAssignment(taskAssignment);
        }

        private String ownerUrl(ConnectorTaskId id) {
            String ownerId = taskOwners.get(id);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

        private String ownerUrl(String connector) {
            String ownerId = connectorOwners.get(connector);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

    }

    public static class ConnectorsAndTasks {
        public static final ConnectorsAndTasks EMPTY =
                new ConnectorsAndTasks(Collections.emptyList(), Collections.emptyList());

        private final Collection<String> connectors;
        private final Collection<ConnectorTaskId> tasks;

        private ConnectorsAndTasks(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            this.connectors = connectors;
            this.tasks = tasks;
        }

        public static class Builder {
            private Collection<String> withConnectors;
            private Collection<ConnectorTaskId> withTasks;

            public Builder() {
            }

            public ConnectorsAndTasks.Builder withCopies(Collection<String> connectors,
                                                         Collection<ConnectorTaskId> tasks) {
                withConnectors = new ArrayList<>(connectors);
                withTasks = new ArrayList<>(tasks);
                return this;
            }

            public ConnectorsAndTasks.Builder with(Collection<String> connectors,
                                                   Collection<ConnectorTaskId> tasks) {
                withConnectors = new ArrayList<>(connectors);
                withTasks = new ArrayList<>(tasks);
                return this;
            }

            public ConnectorsAndTasks build() {
                return new ConnectorsAndTasks(
                        withConnectors != null ? withConnectors : new ArrayList<>(),
                        withTasks != null ? withTasks : new ArrayList<>());
            }
        }

        public Collection<String> connectors() {
            return connectors;
        }

        public Collection<ConnectorTaskId> tasks() {
            return tasks;
        }

        public int size() {
            return connectors.size() + tasks.size();
        }

        public boolean isEmpty() {
            return connectors.isEmpty() && tasks.isEmpty();
        }

        @Override
        public String toString() {
            return "{ connectorIds=" + connectors + ", taskIds=" + tasks + '}';
        }
    }

    public static class WorkerLoad {
        private final String worker;
        private final Collection<String> connectors;
        private final Collection<ConnectorTaskId> tasks;

        private WorkerLoad(
                String worker,
                Collection<String> connectors,
                Collection<ConnectorTaskId> tasks
        ) {
            this.worker = worker;
            this.connectors = connectors;
            this.tasks = tasks;
        }

        public static class Builder {
            private String withWorker;
            private Collection<String> withConnectors;
            private Collection<ConnectorTaskId> withTasks;

            public Builder(String worker) {
                this.withWorker = Objects.requireNonNull(worker, "worker cannot be null");
            }

            public WorkerLoad.Builder withCopies(Collection<String> connectors,
                                                 Collection<ConnectorTaskId> tasks) {
                withConnectors = new ArrayList<>(
                        Objects.requireNonNull(connectors, "connectors may be empty but not null"));
                withTasks = new ArrayList<>(
                        Objects.requireNonNull(tasks, "tasks may be empty but not null"));
                return this;
            }

            public WorkerLoad.Builder with(Collection<String> connectors,
                                           Collection<ConnectorTaskId> tasks) {
                withConnectors = Objects.requireNonNull(connectors,
                        "connectors may be empty but not null");
                withTasks = Objects.requireNonNull(tasks, "tasks may be empty but not null");
                return this;
            }

            public WorkerLoad build() {
                return new WorkerLoad(
                        withWorker,
                        withConnectors != null ? withConnectors : new ArrayList<>(),
                        withTasks != null ? withTasks : new ArrayList<>());
            }
        }

        public String worker() {
            return worker;
        }

        public Collection<String> connectors() {
            return connectors;
        }

        public Collection<ConnectorTaskId> tasks() {
            return tasks;
        }

        public int connectorsSize() {
            return connectors.size();
        }

        public int tasksSize() {
            return tasks.size();
        }

        public void assign(String connector) {
            connectors.add(connector);
        }

        public void assign(ConnectorTaskId task) {
            tasks.add(task);
        }

        public int size() {
            return connectors.size() + tasks.size();
        }

        public boolean isEmpty() {
            return connectors.isEmpty() && tasks.isEmpty();
        }

        public static Comparator<WorkerLoad> connectorComparator() {
            return (left, right) -> {
                int res = left.connectors.size() - right.connectors.size();
                return res != 0 ? res : left.worker == null
                                        ? right.worker == null ? 0 : -1
                                        : left.worker.compareTo(right.worker);
            };
        }

        public static Comparator<WorkerLoad> taskComparator() {
            return (left, right) -> {
                int res = left.tasks.size() - right.tasks.size();
                return res != 0 ? res : left.worker == null
                                        ? right.worker == null ? 0 : -1
                                        : left.worker.compareTo(right.worker);
            };
        }

        @Override
        public String toString() {
            return "{ worker=" + worker + ", connectorIds=" + connectors + ", taskIds=" + tasks + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WorkerLoad)) {
                return false;
            }
            WorkerLoad that = (WorkerLoad) o;
            return worker.equals(that.worker) &&
                    connectors.equals(that.connectors) &&
                    tasks.equals(that.tasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(worker, connectors, tasks);
        }
    }

}
