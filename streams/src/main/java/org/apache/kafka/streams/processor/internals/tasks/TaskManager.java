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
package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import java.util.Set;

public interface TaskManager {

    /**
     * Get the next processable active task for the requested executor. Once the task is assigned to
     * the requested task executor, it should not be assigned to any other executors until it was
     * returned to the task manager.
     *
     * @param executor the requesting {@link TaskExecutor}
     * @return a processable active task not assigned to any other executors, or null if there is no such task available
     */
    StreamTask assignNextTask(final TaskExecutor executor);

    /**
     * Unassign the stream task so that it can be assigned to other executors later
     * or be removed from the task manager. The requested executor must have
     * the task already, otherwise an exception would be thrown.
     *
     * @param executor the requesting {@link TaskExecutor}
     */
    void unassignTask(final StreamTask task, final TaskExecutor executor);

    /**
     * Lock a set of active tasks from the task manager so that they will not be assigned to
     * any {@link TaskExecutor}s anymore until they are unlocked. At the time this function
     * is called, the requested tasks may already be locked by some {@link TaskExecutor}s,
     * and in that case the task manager need to first unassign these tasks from the
     * executors.
     *
     * This function is needed when we need to 1) commit these tasks, 2) remove these tasks.
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<Void> lockTasks(final Set<TaskId> taskIds);

    /**
     * Lock all of the managed active tasks from the task manager. Similar to {@link #lockTasks(Set)}.
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<Void> lockAllTasks();

    /**
     * Unlock the tasks so that they can be assigned to executors
     */
    void unlockTasks(final Set<TaskId> taskIds);

    /**
     * Unlock all of the managed active tasks from the task manager. Similar to {@link #unlockTasks(Set)}.
     *
     * This method does not block, instead a future is returned.
     */
    void unlockAllTasks();

    /**
     * Add a new active task to the task manager.
     *
     * @param tasks task to add
     */
    void add(final Set<StreamTask> tasks);

    /**
     * Remove an active task from the task manager.
     *
     * The task to remove must be locked.
     *
     * @param taskId ID of the task to remove
     */
    void remove(final TaskId taskId);

    /**
     * Gets all active tasks that are managed by this manager. The returned tasks are read-only
     * and cannot be manipulated.
     *
     * @return set of all managed active tasks
     */
    Set<ReadOnlyTask> getTasks();
}
