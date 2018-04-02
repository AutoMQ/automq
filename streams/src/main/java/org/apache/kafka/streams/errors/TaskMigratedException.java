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
package org.apache.kafka.streams.errors;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.internals.Task;

/**
 * Indicates that a task got migrated to another thread.
 * Thus, the task raising this exception can be cleaned up and closed as "zombie".
 */
public class TaskMigratedException extends StreamsException {

    private final static long serialVersionUID = 1L;

    private final Task task;

    // this is for unit test only
    public TaskMigratedException() {
        super("A task has been migrated unexpectedly", null);

        this.task = null;
    }

    public TaskMigratedException(final Task task,
                                 final TopicPartition topicPartition,
                                 final long endOffset,
                                 final long pos) {
        super(String.format("Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
                            topicPartition,
                            endOffset,
                            pos),
            null);

        this.task = task;
    }

    public TaskMigratedException(final Task task) {
        super(String.format("Task %s is unexpectedly closed during processing", task.id()), null);

        this.task = task;
    }

    public TaskMigratedException(final Task task,
                                 final Throwable throwable) {
        super(String.format("Client request for task %s has been fenced due to a rebalance", task.id()), throwable);

        this.task = task;
    }

    public Task migratedTask() {
        return task;
    }

}
