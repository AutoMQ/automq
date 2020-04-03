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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskMovement {
    private static final Logger log = LoggerFactory.getLogger(TaskMovement.class);

    final TaskId task;
    final UUID source;
    final UUID destination;

    TaskMovement(final TaskId task, final UUID source, final UUID destination) {
        this.task = task;
        this.source = source;
        this.destination = destination;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TaskMovement movement = (TaskMovement) o;
        return Objects.equals(task, movement.task) &&
                   Objects.equals(source, movement.source) &&
                   Objects.equals(destination, movement.destination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task, source, destination);
    }

    /**
     * Returns a list of the movements of tasks from statefulActiveTaskAssignment to balancedStatefulActiveTaskAssignment
     * @param statefulActiveTaskAssignment the initial assignment, with source clients
     * @param balancedStatefulActiveTaskAssignment the final assignment, with destination clients
     */
    static List<TaskMovement> getMovements(final Map<UUID, List<TaskId>> statefulActiveTaskAssignment,
        final Map<UUID, List<TaskId>> balancedStatefulActiveTaskAssignment,
        final int maxWarmupReplicas) {
        if (statefulActiveTaskAssignment.size() != balancedStatefulActiveTaskAssignment.size()) {
            throw new IllegalStateException("Tried to compute movements but assignments differ in size.");
        }

        final Map<TaskId, UUID> taskToDestinationClient = new HashMap<>();
        for (final Map.Entry<UUID, List<TaskId>> clientEntry : balancedStatefulActiveTaskAssignment.entrySet()) {
            final UUID destination = clientEntry.getKey();
            for (final TaskId task : clientEntry.getValue()) {
                taskToDestinationClient.put(task, destination);
            }
        }

        final List<TaskMovement> movements = new LinkedList<>();
        for (final Map.Entry<UUID, List<TaskId>> sourceClientEntry : statefulActiveTaskAssignment.entrySet()) {
            final UUID source = sourceClientEntry.getKey();

            for (final TaskId task : sourceClientEntry.getValue()) {
                final UUID destination = taskToDestinationClient.get(task);
                if (destination == null) {
                    log.error("Task {} is assigned to client {} in initial assignment but has no owner in the final " +
                                  "balanced assignment.", task, source);
                    throw new IllegalStateException("Found task in initial assignment that was not assigned in the final.");
                } else if (!source.equals(destination)) {
                    movements.add(new TaskMovement(task, source, destination));
                    if (movements.size() == maxWarmupReplicas) {
                        return movements;
                    }
                }
            }
        }
        return movements;
    }
}