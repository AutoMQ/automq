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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class StickyTaskAssignor<ID> implements TaskAssignor<ID, TaskId> {

    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);
    private final Map<ID, ClientState> clients;
    private final Set<TaskId> taskIds;
    private final Map<TaskId, ID> previousActiveTaskAssignment = new HashMap<>();
    private final Map<TaskId, Set<ID>> previousStandbyTaskAssignment = new HashMap<>();
    private final TaskPairs taskPairs;

    public StickyTaskAssignor(final Map<ID, ClientState> clients, final Set<TaskId> taskIds) {
        this.clients = clients;
        this.taskIds = taskIds;
        taskPairs = new TaskPairs(taskIds.size() * (taskIds.size() - 1) / 2);
        mapPreviousTaskAssignment(clients);
    }

    @Override
    public void assign(final int numStandbyReplicas) {
        assignActive();
        assignStandby(numStandbyReplicas);
    }

    private void assignStandby(final int numStandbyReplicas) {
        for (final TaskId taskId : taskIds) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<ID> ids = findClientsWithoutAssignedTask(taskId);
                if (ids.isEmpty()) {
                    log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                                     "There is not enough available capacity. You should " +
                                     "increase the number of threads and/or application instances " +
                                     "to maintain the requested number of standby replicas.",
                             numStandbyReplicas - i,
                             numStandbyReplicas, taskId);
                    break;
                }
                allocateTaskWithClientCandidates(taskId, ids, false);
            }
        }
    }

    private void assignActive() {
        final int totalCapacity = sumCapacity(clients.values());
        final int tasksPerThread = taskIds.size() / totalCapacity;
        final Set<TaskId> assigned = new HashSet<>();

        // first try and re-assign existing active tasks to clients that previously had
        // the same active task
        for (final Map.Entry<TaskId, ID> entry : previousActiveTaskAssignment.entrySet()) {
            final TaskId taskId = entry.getKey();
            if (taskIds.contains(taskId)) {
                final ClientState client = clients.get(entry.getValue());
                if (client.hasUnfulfilledQuota(tasksPerThread)) {
                    assignTaskToClient(assigned, taskId, client);
                }
            }
        }

        final Set<TaskId> unassigned = new HashSet<>(taskIds);
        unassigned.removeAll(assigned);

        // try and assign any remaining unassigned tasks to clients that previously
        // have seen the task.
        for (final Iterator<TaskId> iterator = unassigned.iterator(); iterator.hasNext(); ) {
            final TaskId taskId = iterator.next();
            final Set<ID> clientIds = previousStandbyTaskAssignment.get(taskId);
            if (clientIds != null) {
                for (final ID clientId : clientIds) {
                    final ClientState client = clients.get(clientId);
                    if (client.hasUnfulfilledQuota(tasksPerThread)) {
                        assignTaskToClient(assigned, taskId, client);
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        // assign any remaining unassigned tasks
        final List<TaskId> sortedTasks = new ArrayList<>(unassigned);
        Collections.sort(sortedTasks);
        for (final TaskId taskId : sortedTasks) {
            allocateTaskWithClientCandidates(taskId, clients.keySet(), true);
        }
    }

    private void allocateTaskWithClientCandidates(final TaskId taskId, final Set<ID> clientsWithin, final boolean active) {
        final ClientState client = findClient(taskId, clientsWithin, active);
        taskPairs.addPairs(taskId, client.assignedTasks());
        client.assign(taskId, active);
    }

    private void assignTaskToClient(final Set<TaskId> assigned, final TaskId taskId, final ClientState client) {
        taskPairs.addPairs(taskId, client.assignedTasks());
        client.assign(taskId, true);
        assigned.add(taskId);
    }

    private Set<ID> findClientsWithoutAssignedTask(final TaskId taskId) {
        final Set<ID> clientIds = new HashSet<>();
        for (final Map.Entry<ID, ClientState> client : clients.entrySet()) {
            if (!client.getValue().hasAssignedTask(taskId)) {
                clientIds.add(client.getKey());
            }
        }
        return clientIds;
    }


    private ClientState findClient(final TaskId taskId, final Set<ID> clientsWithin, final boolean active) {

        // optimize the case where there is only 1 id to search within.
        if (clientsWithin.size() == 1) {
            return clients.get(clientsWithin.iterator().next());
        }

        final ClientState previous = findClientsWithPreviousAssignedTask(taskId, clientsWithin);
        if (previous == null) {
            return leastLoaded(taskId, clientsWithin, active);
        }

        if (shouldBalanceLoad(previous)) {
            final ClientState standby = findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
            if (standby == null
                    || shouldBalanceLoad(standby)) {
                return leastLoaded(taskId, clientsWithin, active);
            }
            return standby;
        }

        return previous;
    }

    private boolean shouldBalanceLoad(final ClientState client) {
        return client.reachedCapacity() && hasClientsWithMoreAvailableCapacity(client);
    }

    private boolean hasClientsWithMoreAvailableCapacity(final ClientState client) {
        for (final ClientState clientState : clients.values()) {
            if (clientState.hasMoreAvailableCapacityThan(client)) {
                return true;
            }
        }
        return false;
    }

    private ClientState findClientsWithPreviousAssignedTask(final TaskId taskId,
                                                                    final Set<ID> clientsWithin) {
        final ID previous = previousActiveTaskAssignment.get(taskId);
        if (previous != null && clientsWithin.contains(previous)) {
            return clients.get(previous);
        }
        return findLeastLoadedClientWithPreviousStandByTask(taskId, clientsWithin);
    }

    private ClientState findLeastLoadedClientWithPreviousStandByTask(final TaskId taskId, final Set<ID> clientsWithin) {
        final Set<ID> ids = previousStandbyTaskAssignment.get(taskId);
        if (ids == null) {
            return null;
        }
        final HashSet<ID> constrainTo = new HashSet<>(ids);
        constrainTo.retainAll(clientsWithin);
        return leastLoaded(taskId, constrainTo, false);
    }

    private ClientState leastLoaded(final TaskId taskId, final Set<ID> clientIds, final boolean active) {
        final ClientState leastLoaded = findLeastLoaded(taskId, clientIds, true, active);
        if (leastLoaded == null) {
            return findLeastLoaded(taskId, clientIds, false, active);
        }
        return leastLoaded;
    }

    private ClientState findLeastLoaded(final TaskId taskId,
                                        final Set<ID> clientIds,
                                        final boolean checkTaskPairs,
                                        final boolean active) {
        ClientState leastLoaded = null;
        for (final ID id : clientIds) {
            final ClientState client = clients.get(id);
            if (client.assignedTaskCount() == 0) {
                return client;
            }

            if (leastLoaded == null || client.hasMoreAvailableCapacityThan(leastLoaded)) {
                if (!checkTaskPairs) {
                    leastLoaded = client;
                } else if (taskPairs.hasNewPair(taskId, client.assignedTasks(), active)) {
                    leastLoaded = client;
                }
            }

        }
        return leastLoaded;

    }

    private void mapPreviousTaskAssignment(final Map<ID, ClientState> clients) {
        for (final Map.Entry<ID, ClientState> clientState : clients.entrySet()) {
            for (final TaskId activeTask : clientState.getValue().previousActiveTasks()) {
                previousActiveTaskAssignment.put(activeTask, clientState.getKey());
            }

            for (final TaskId prevAssignedTask : clientState.getValue().previousStandbyTasks()) {
                if (!previousStandbyTaskAssignment.containsKey(prevAssignedTask)) {
                    previousStandbyTaskAssignment.put(prevAssignedTask, new HashSet<ID>());
                }
                previousStandbyTaskAssignment.get(prevAssignedTask).add(clientState.getKey());
            }
        }

    }

    private int sumCapacity(final Collection<ClientState> values) {
        int capacity = 0;
        for (final ClientState client : values) {
            capacity += client.capacity();
        }
        return capacity;
    }

    private static class TaskPairs {
        private final Set<Pair> pairs;
        private final int maxPairs;

        TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        boolean hasNewPair(final TaskId task1,
                           final Set<TaskId> taskIds,
                           final boolean active) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            for (final TaskId taskId : taskIds) {
                if (!active && !pairs.contains(pair(task1, taskId))) {
                    return true;
                }
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                pairs.add(pair(id, taskId));
            }
        }

        Pair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }

        private static class Pair {
            private final TaskId task1;
            private final TaskId task2;

            Pair(final TaskId task1, final TaskId task2) {
                this.task1 = task1;
                this.task2 = task2;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                final Pair pair = (Pair) o;
                return Objects.equals(task1, pair.task1) &&
                        Objects.equals(task2, pair.task2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(task1, task2);
            }
        }


    }

}
