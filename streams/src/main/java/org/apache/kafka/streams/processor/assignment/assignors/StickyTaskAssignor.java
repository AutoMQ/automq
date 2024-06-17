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
package org.apache.kafka.streams.processor.assignment.assignors;

import static java.util.Collections.unmodifiableMap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StickyTaskAssignor implements TaskAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(StickyTaskAssignor.class);

    private final boolean mustPreserveActiveTaskAssignment;

    public StickyTaskAssignor() {
        this(false);
    }

    public StickyTaskAssignor(final boolean mustPreserveActiveTaskAssignment) {
        this.mustPreserveActiveTaskAssignment = mustPreserveActiveTaskAssignment;
    }

    @Override
    public TaskAssignment assign(final ApplicationState applicationState) {
        final Map<ProcessId, KafkaStreamsState> clients = applicationState.kafkaStreamsStates(false);
        final Map<TaskId, ProcessId> previousActiveAssignment = mapPreviousActiveTasks(clients);
        final Map<TaskId, Set<ProcessId>> previousStandbyAssignment = mapPreviousStandbyTasks(clients);
        final AssignmentState assignmentState = new AssignmentState(applicationState, clients,
            previousActiveAssignment, previousStandbyAssignment);

        assignActive(applicationState, clients.values(), assignmentState, this.mustPreserveActiveTaskAssignment);
        optimizeActive(applicationState, assignmentState);
        assignStandby(applicationState, assignmentState);
        optimizeStandby(applicationState, assignmentState);

        final Map<ProcessId, KafkaStreamsAssignment> finalAssignments = assignmentState.buildKafkaStreamsAssignments();
        if (mustPreserveActiveTaskAssignment && !finalAssignments.isEmpty()) {
            // We set the followup deadline for only one of the clients.
            final ProcessId clientId = finalAssignments.entrySet().iterator().next().getKey();
            final KafkaStreamsAssignment previousAssignment = finalAssignments.get(clientId);
            finalAssignments.put(clientId, previousAssignment.withFollowupRebalance(Instant.ofEpochMilli(0)));
        }

        return new TaskAssignment(finalAssignments.values());
    }

    private void optimizeActive(final ApplicationState applicationState,
                                final AssignmentState assignmentState) {
        if (mustPreserveActiveTaskAssignment) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> currentAssignments = assignmentState.buildKafkaStreamsAssignments();

        final Set<TaskId> statefulTasks = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .map(TaskInfo::id)
            .collect(Collectors.toSet());
        final Map<ProcessId, KafkaStreamsAssignment> optimizedAssignmentsForStatefulTasks = TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            applicationState, currentAssignments, new TreeSet<>(statefulTasks));

        final Set<TaskId> statelessTasks = applicationState.allTasks().values().stream()
            .filter(task -> !task.isStateful())
            .map(TaskInfo::id)
            .collect(Collectors.toSet());
        final Map<ProcessId, KafkaStreamsAssignment> optimizedAssignmentsForAllTasks = TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            applicationState, optimizedAssignmentsForStatefulTasks, new TreeSet<>(statelessTasks));

        assignmentState.processOptimizedAssignments(optimizedAssignmentsForAllTasks);
    }

    private void optimizeStandby(final ApplicationState applicationState, final AssignmentState assignmentState) {
        if (applicationState.assignmentConfigs().numStandbyReplicas() <= 0) {
            return;
        }

        if (mustPreserveActiveTaskAssignment) {
            return;
        }

        final Map<ProcessId, KafkaStreamsAssignment> currentAssignments = assignmentState.buildKafkaStreamsAssignments();
        final Map<ProcessId, KafkaStreamsAssignment> optimizedAssignments = TaskAssignmentUtils.optimizeRackAwareStandbyTasks(
            applicationState, currentAssignments);
        assignmentState.processOptimizedAssignments(optimizedAssignments);
    }

    private static void assignActive(final ApplicationState applicationState,
                                     final Collection<KafkaStreamsState> clients,
                                     final AssignmentState assignmentState,
                                     final boolean mustPreserveActiveTaskAssignment) {
        final int totalCapacity = computeTotalProcessingThreads(clients);
        final Set<TaskId> allTaskIds = applicationState.allTasks().keySet();
        final int taskCount = allTaskIds.size();
        final int activeTasksPerThread = taskCount / totalCapacity;
        final Set<TaskId> unassigned = new HashSet<>(allTaskIds);

        // first try and re-assign existing active tasks to clients that previously had
        // the same active task
        for (final TaskId taskId : assignmentState.previousActiveAssignment.keySet()) {
            final ProcessId previousClientForTask = assignmentState.previousActiveAssignment.get(taskId);
            if (allTaskIds.contains(taskId)) {
                if (mustPreserveActiveTaskAssignment || assignmentState.hasRoomForActiveTask(previousClientForTask, activeTasksPerThread)) {
                    assignmentState.finalizeAssignment(taskId, previousClientForTask, AssignedTask.Type.ACTIVE);
                    unassigned.remove(taskId);
                }
            }
        }

        // try and assign any remaining unassigned tasks to clients that previously
        // have seen the task.
        for (final Iterator<TaskId> iterator = unassigned.iterator(); iterator.hasNext(); ) {
            final TaskId taskId = iterator.next();
            final Set<ProcessId> previousClientsForStandbyTask = assignmentState.previousStandbyAssignment.getOrDefault(taskId, new HashSet<>());
            for (final ProcessId client: previousClientsForStandbyTask) {
                if (assignmentState.hasRoomForActiveTask(client, activeTasksPerThread)) {
                    assignmentState.finalizeAssignment(taskId, client, AssignedTask.Type.ACTIVE);
                    iterator.remove();
                    break;
                }
            }
        }

        // assign any remaining unassigned tasks
        final List<TaskId> sortedTasks = new ArrayList<>(unassigned);
        Collections.sort(sortedTasks);
        for (final TaskId taskId : sortedTasks) {
            final Set<ProcessId> candidateClients = clients.stream()
                .map(KafkaStreamsState::processId)
                .collect(Collectors.toSet());
            final ProcessId bestClient = assignmentState.findBestClientForTask(taskId, candidateClients);
            assignmentState.finalizeAssignment(taskId, bestClient, AssignedTask.Type.ACTIVE);
        }
    }

    private static void assignStandby(final ApplicationState applicationState,
                                      final AssignmentState assignmentState) {
        final Set<TaskInfo> statefulTasks = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .collect(Collectors.toSet());
        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        for (final TaskInfo task : statefulTasks) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<ProcessId> candidateClients = assignmentState.findClientsWithoutAssignedTask(task.id());
                if (candidateClients.isEmpty()) {
                    LOG.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                             "There is not enough available capacity. You should " +
                             "increase the number of threads and/or application instances " +
                             "to maintain the requested number of standby replicas.",
                        numStandbyReplicas - i,
                        numStandbyReplicas, task.id());
                    break;
                }

                final ProcessId bestClient = assignmentState.findBestClientForTask(task.id(), candidateClients);
                assignmentState.finalizeAssignment(task.id(), bestClient, AssignedTask.Type.STANDBY);
            }
        }
    }

    private static Map<TaskId, ProcessId> mapPreviousActiveTasks(final Map<ProcessId, KafkaStreamsState> clients) {
        final Map<TaskId, ProcessId> previousActiveTasks = new HashMap<>();
        for (final KafkaStreamsState client : clients.values()) {
            for (final TaskId taskId : client.previousActiveTasks()) {
                previousActiveTasks.put(taskId, client.processId());
            }
        }
        return previousActiveTasks;
    }

    private static Map<TaskId, Set<ProcessId>> mapPreviousStandbyTasks(final Map<ProcessId, KafkaStreamsState> clients) {
        final Map<TaskId, Set<ProcessId>> previousStandbyTasks = new HashMap<>();
        for (final KafkaStreamsState client : clients.values()) {
            for (final TaskId taskId : client.previousActiveTasks()) {
                previousStandbyTasks.computeIfAbsent(taskId, k -> new HashSet<>());
                previousStandbyTasks.get(taskId).add(client.processId());
            }
        }
        return previousStandbyTasks;
    }

    private static int computeTotalProcessingThreads(final Collection<KafkaStreamsState> clients) {
        int count = 0;
        for (final KafkaStreamsState client : clients) {
            count += client.numProcessingThreads();
        }
        return count;
    }

    private static class AssignmentState {
        private final Map<ProcessId, KafkaStreamsState> clients;
        private final Map<TaskId, ProcessId> previousActiveAssignment;
        private final Map<TaskId, Set<ProcessId>> previousStandbyAssignment;

        private final TaskPairs taskPairs;

        private Map<TaskId, Set<ProcessId>> newTaskLocations;
        private Map<ProcessId, Set<AssignedTask>> newAssignments;

        private AssignmentState(final ApplicationState applicationState,
                                final Map<ProcessId, KafkaStreamsState> clients,
                                final Map<TaskId, ProcessId> previousActiveAssignment,
                                final Map<TaskId, Set<ProcessId>> previousStandbyAssignment) {
            this.clients = clients;
            this.previousActiveAssignment = unmodifiableMap(previousActiveAssignment);
            this.previousStandbyAssignment = unmodifiableMap(previousStandbyAssignment);

            final int taskCount = applicationState.allTasks().size();
            final int maxPairs = taskCount * (taskCount - 1) / 2;
            this.taskPairs = new TaskPairs(maxPairs);

            this.newTaskLocations = new HashMap<>();
            this.newAssignments = new HashMap<>();
        }

        public void finalizeAssignment(final TaskId taskId, final ProcessId client, final AssignedTask.Type type) {
            newAssignments.computeIfAbsent(client, k -> new HashSet<>());
            newTaskLocations.computeIfAbsent(taskId, k -> new HashSet<>());

            final Set<TaskId> newAssignmentsForClient = newAssignments.get(client)
                .stream().map(AssignedTask::id).collect(Collectors.toSet());

            taskPairs.addPairs(taskId, newAssignmentsForClient);
            newAssignments.get(client).add(new AssignedTask(taskId, type));
            newTaskLocations.get(taskId).add(client);
        }

        public Map<ProcessId, KafkaStreamsAssignment> buildKafkaStreamsAssignments() {
            final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments = new HashMap<>();
            for (final Map.Entry<ProcessId, Set<AssignedTask>> entry : newAssignments.entrySet()) {
                final ProcessId processId = entry.getKey();
                final Set<AssignedTask> assignedTasks = newAssignments.get(processId);
                final KafkaStreamsAssignment assignment = KafkaStreamsAssignment.of(processId, assignedTasks);
                kafkaStreamsAssignments.put(processId, assignment);
            }
            return kafkaStreamsAssignments;
        }

        public void processOptimizedAssignments(final Map<ProcessId, KafkaStreamsAssignment> optimizedAssignments) {
            final Map<TaskId, Set<ProcessId>> newTaskLocations = new HashMap<>();
            final Map<ProcessId, Set<AssignedTask>> newAssignments = new HashMap<>();

            for (final Map.Entry<ProcessId, KafkaStreamsAssignment> entry : optimizedAssignments.entrySet()) {
                final ProcessId processId = entry.getKey();
                final Set<AssignedTask> assignedTasks = new HashSet<>(optimizedAssignments.get(processId).tasks().values());
                newAssignments.put(processId, assignedTasks);

                for (final AssignedTask task : assignedTasks) {
                    newTaskLocations.computeIfAbsent(task.id(), k -> new HashSet<>());
                    newTaskLocations.get(task.id()).add(processId);
                }
            }

            this.newTaskLocations = newTaskLocations;
            this.newAssignments = newAssignments;
        }

        public boolean hasRoomForActiveTask(final ProcessId processId, final int activeTasksPerThread) {
            final int capacity = clients.get(processId).numProcessingThreads();
            final int newActiveTaskCount = newAssignments.computeIfAbsent(processId, k -> new HashSet<>())
                .stream().filter(assignedTask -> assignedTask.type() == AssignedTask.Type.ACTIVE)
                .collect(Collectors.toSet())
                .size();
            return newActiveTaskCount < capacity * activeTasksPerThread;
        }

        public ProcessId findBestClientForTask(final TaskId taskId, final Set<ProcessId> clientsWithin) {
            if (clientsWithin.size() == 1) {
                return clientsWithin.iterator().next();
            }

            final ProcessId previousClient = findLeastLoadedClientWithPreviousActiveOrStandbyTask(
                taskId, clientsWithin);
            if (previousClient == null) {
                return findLeastLoadedClient(taskId, clientsWithin);
            }

            if (shouldBalanceLoad(previousClient)) {
                final ProcessId standby = findLeastLoadedClientWithPreviousStandbyTask(taskId, clientsWithin);
                if (standby == null || shouldBalanceLoad(standby)) {
                    return findLeastLoadedClient(taskId, clientsWithin);
                }
                return standby;
            }
            return previousClient;
        }

        public Set<ProcessId> findClientsWithoutAssignedTask(final TaskId taskId) {
            final Set<ProcessId> unavailableClients = newTaskLocations.computeIfAbsent(taskId, k -> new HashSet<>());
            return clients.values().stream()
                .map(KafkaStreamsState::processId)
                .filter(o -> !unavailableClients.contains(o))
                .collect(Collectors.toSet());
        }

        public double clientLoad(final ProcessId processId) {
            final int capacity = clients.get(processId).numProcessingThreads();
            final double totalTaskCount = newAssignments.getOrDefault(processId, new HashSet<>()).size();
            return totalTaskCount / capacity;
        }

        public ProcessId findLeastLoadedClient(final TaskId taskId, final Set<ProcessId> clientIds) {
            ProcessId leastLoaded = null;
            for (final ProcessId processId : clientIds) {
                final double thisClientLoad = clientLoad(processId);
                if (thisClientLoad == 0) {
                    return processId;
                }

                if (leastLoaded == null || thisClientLoad < clientLoad(leastLoaded)) {
                    final Set<TaskId> assignedTasks = newAssignments.getOrDefault(processId, new HashSet<>())
                        .stream().map(AssignedTask::id).collect(Collectors.toSet());
                    if (taskPairs.hasNewPair(taskId, assignedTasks)) {
                        leastLoaded = processId;
                    }
                }
            }

            if (leastLoaded != null) {
                return leastLoaded;
            }

            for (final ProcessId processId : clientIds) {
                final double thisClientLoad = clientLoad(processId);

                if (leastLoaded == null || thisClientLoad < clientLoad(leastLoaded)) {
                    leastLoaded = processId;
                }
            }

            return leastLoaded;
        }

        public ProcessId findLeastLoadedClientWithPreviousActiveOrStandbyTask(final TaskId taskId,
                                                                              final Set<ProcessId> clientsWithin) {
            final ProcessId previous = previousActiveAssignment.get(taskId);
            if (previous != null && clientsWithin.contains(previous)) {
                return previous;
            }
            return findLeastLoadedClientWithPreviousStandbyTask(taskId, clientsWithin);
        }

        public ProcessId findLeastLoadedClientWithPreviousStandbyTask(final TaskId taskId,
                                                                      final Set<ProcessId> clientsWithin) {
            final Set<ProcessId> ids = previousStandbyAssignment.getOrDefault(taskId, new HashSet<>());
            final HashSet<ProcessId> constrainTo = new HashSet<>(ids);
            constrainTo.retainAll(clientsWithin);
            return findLeastLoadedClient(taskId, constrainTo);
        }

        public boolean shouldBalanceLoad(final ProcessId client) {
            final double thisClientLoad = clientLoad(client);
            if (thisClientLoad < 1) {
                return false;
            }

            for (final ProcessId otherClient : clients.keySet()) {
                if (clientLoad(otherClient) < thisClientLoad) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class TaskPairs {
        private final Set<TaskPair> pairs;
        private final int maxPairs;

        public TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        public boolean hasNewPair(final TaskId task1,
                                  final Set<TaskId> taskIds) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            for (final TaskId taskId : taskIds) {
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        public void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                pairs.add(pair(id, taskId));
            }
        }

        public TaskPair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new TaskPair(task1, task2);
            }
            return new TaskPair(task2, task1);
        }
    }

    private static class TaskPair {
        private final TaskId task1;
        private final TaskId task2;

        TaskPair(final TaskId task1, final TaskId task2) {
            this.task1 = task1;
            this.task2 = task2;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TaskPair pair = (TaskPair) o;
            return Objects.equals(task1, pair.task1) &&
                   Objects.equals(task2, pair.task2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task1, task2);
        }
    }
}
