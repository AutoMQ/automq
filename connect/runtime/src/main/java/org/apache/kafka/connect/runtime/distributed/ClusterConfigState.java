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

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * An immutable snapshot of the configuration state of connectors and tasks in a Kafka Connect cluster.
 */
public class ClusterConfigState {
    public static final long NO_OFFSET = -1;
    public static final ClusterConfigState EMPTY = new ClusterConfigState(
            NO_OFFSET,
            null,
            Collections.<String, Integer>emptyMap(),
            Collections.<String, Map<String, String>>emptyMap(),
            Collections.<String, TargetState>emptyMap(),
            Collections.<ConnectorTaskId, Map<String, String>>emptyMap(),
            Collections.<String>emptySet());

    private final long offset;
    private final SessionKey sessionKey;
    private final Map<String, Integer> connectorTaskCounts;
    private final Map<String, Map<String, String>> connectorConfigs;
    private final Map<String, TargetState> connectorTargetStates;
    private final Map<ConnectorTaskId, Map<String, String>> taskConfigs;
    private final Set<String> inconsistentConnectors;
    private final WorkerConfigTransformer configTransformer;

    public ClusterConfigState(long offset,
                              SessionKey sessionKey,
                              Map<String, Integer> connectorTaskCounts,
                              Map<String, Map<String, String>> connectorConfigs,
                              Map<String, TargetState> connectorTargetStates,
                              Map<ConnectorTaskId, Map<String, String>> taskConfigs,
                              Set<String> inconsistentConnectors) {
        this(offset,
                sessionKey,
                connectorTaskCounts,
                connectorConfigs,
                connectorTargetStates,
                taskConfigs,
                inconsistentConnectors,
                null);
    }

    public ClusterConfigState(long offset,
                              SessionKey sessionKey,
                              Map<String, Integer> connectorTaskCounts,
                              Map<String, Map<String, String>> connectorConfigs,
                              Map<String, TargetState> connectorTargetStates,
                              Map<ConnectorTaskId, Map<String, String>> taskConfigs,
                              Set<String> inconsistentConnectors,
                              WorkerConfigTransformer configTransformer) {
        this.offset = offset;
        this.sessionKey = sessionKey;
        this.connectorTaskCounts = connectorTaskCounts;
        this.connectorConfigs = connectorConfigs;
        this.connectorTargetStates = connectorTargetStates;
        this.taskConfigs = taskConfigs;
        this.inconsistentConnectors = inconsistentConnectors;
        this.configTransformer = configTransformer;
    }

    /**
     * Get the last offset read to generate this config state. This offset is not guaranteed to be perfectly consistent
     * with the recorded state because some partial updates to task configs may have been read.
     * @return the latest config offset
     */
    public long offset() {
        return offset;
    }

    /**
     * Get the latest session key from the config state
     * @return the {@link SessionKey session key}; may be null if no key has been read yet
     */
    public SessionKey sessionKey() {
        return sessionKey;
    }

    /**
     * Check whether this snapshot contains configuration for a connector.
     * @param connector name of the connector
     * @return true if this state contains configuration for the connector, false otherwise
     */
    public boolean contains(String connector) {
        return connectorConfigs.containsKey(connector);
    }

    /**
     * Get a list of the connectors in this configuration
     */
    public Set<String> connectors() {
        return connectorConfigs.keySet();
    }

    /**
     * Get the configuration for a connector.  The configuration will have been transformed by
     * {@link org.apache.kafka.common.config.ConfigTransformer} by having all variable
     * references replaced with the current values from external instances of
     * {@link ConfigProvider}, and may include secrets.
     * @param connector name of the connector
     * @return a map containing configuration parameters
     */
    public Map<String, String> connectorConfig(String connector) {
        Map<String, String> configs = connectorConfigs.get(connector);
        if (configTransformer != null) {
            configs = configTransformer.transform(connector, configs);
        }
        return configs;
    }

    public Map<String, String> rawConnectorConfig(String connector) {
        return connectorConfigs.get(connector);
    }

    /**
     * Get the target state of the connector
     * @param connector name of the connector
     * @return the target state
     */
    public TargetState targetState(String connector) {
        return connectorTargetStates.get(connector);
    }

    /**
     * Get the configuration for a task.  The configuration will have been transformed by
     * {@link org.apache.kafka.common.config.ConfigTransformer} by having all variable
     * references replaced with the current values from external instances of
     * {@link ConfigProvider}, and may include secrets.
     * @param task id of the task
     * @return a map containing configuration parameters
     */
    public Map<String, String> taskConfig(ConnectorTaskId task) {
        Map<String, String> configs = taskConfigs.get(task);
        if (configTransformer != null) {
            configs = configTransformer.transform(task.connector(), configs);
        }
        return configs;
    }

    public Map<String, String> rawTaskConfig(ConnectorTaskId task) {
        return taskConfigs.get(task);
    }

    /**
     * Get all task configs for a connector.  The configurations will have been transformed by
     * {@link org.apache.kafka.common.config.ConfigTransformer} by having all variable
     * references replaced with the current values from external instances of
     * {@link ConfigProvider}, and may include secrets.
     * @param connector name of the connector
     * @return a list of task configurations
     */
    public List<Map<String, String>> allTaskConfigs(String connector) {
        Map<Integer, Map<String, String>> taskConfigs = new TreeMap<>();
        for (Map.Entry<ConnectorTaskId, Map<String, String>> taskConfigEntry : this.taskConfigs.entrySet()) {
            if (taskConfigEntry.getKey().connector().equals(connector)) {
                Map<String, String> configs = taskConfigEntry.getValue();
                if (configTransformer != null) {
                    configs = configTransformer.transform(connector, configs);
                }
                taskConfigs.put(taskConfigEntry.getKey().task(), configs);
            }
        }
        return new LinkedList<>(taskConfigs.values());
    }

    /**
     * Get the number of tasks assigned for the given connector.
     * @param connectorName name of the connector to look up tasks for
     * @return the number of tasks
     */
    public int taskCount(String connectorName) {
        Integer count = connectorTaskCounts.get(connectorName);
        return count == null ? 0 : count;
    }

    /**
     * Get the current set of task IDs for the specified connector.
     * @param connectorName the name of the connector to look up task configs for
     * @return the current set of connector task IDs
     */
    public List<ConnectorTaskId> tasks(String connectorName) {
        if (inconsistentConnectors.contains(connectorName))
            return Collections.emptyList();

        Integer numTasks = connectorTaskCounts.get(connectorName);
        if (numTasks == null)
            return Collections.emptyList();

        List<ConnectorTaskId> taskIds = new ArrayList<>();
        for (int taskIndex = 0; taskIndex < numTasks; taskIndex++) {
            ConnectorTaskId taskId = new ConnectorTaskId(connectorName, taskIndex);
            taskIds.add(taskId);
        }
        return taskIds;
    }

    /**
     * Get the set of connectors which have inconsistent data in this snapshot. These inconsistencies can occur due to
     * partially completed writes combined with log compaction.
     *
     * Connectors in this set will appear in the output of {@link #connectors()} since their connector configuration is
     * available, but not in the output of {@link #taskConfig(ConnectorTaskId)} since the task configs are incomplete.
     *
     * When a worker detects a connector in this state, it should request that the connector regenerate its task
     * configurations.
     *
     * @return the set of inconsistent connectors
     */
    public Set<String> inconsistentConnectors() {
        return inconsistentConnectors;
    }

    @Override
    public String toString() {
        return "ClusterConfigState{" +
                "offset=" + offset +
                ", sessionKey=" + (sessionKey != null ? "[hidden]" : "null") +
                ", connectorTaskCounts=" + connectorTaskCounts +
                ", connectorConfigs=" + connectorConfigs +
                ", taskConfigs=" + taskConfigs +
                ", inconsistentConnectors=" + inconsistentConnectors +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterConfigState that = (ClusterConfigState) o;
        return offset == that.offset &&
                Objects.equals(sessionKey, that.sessionKey) &&
                Objects.equals(connectorTaskCounts, that.connectorTaskCounts) &&
                Objects.equals(connectorConfigs, that.connectorConfigs) &&
                Objects.equals(connectorTargetStates, that.connectorTargetStates) &&
                Objects.equals(taskConfigs, that.taskConfigs) &&
                Objects.equals(inconsistentConnectors, that.inconsistentConnectors) &&
                Objects.equals(configTransformer, that.configTransformer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                offset,
                sessionKey,
                connectorTaskCounts,
                connectorConfigs,
                connectorTargetStates,
                taskConfigs,
                inconsistentConnectors,
                configTransformer);
    }
}
