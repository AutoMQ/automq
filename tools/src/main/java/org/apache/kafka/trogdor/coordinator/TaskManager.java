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

package org.apache.kafka.trogdor.coordinator;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskPending;
import org.apache.kafka.trogdor.rest.TaskRunning;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TaskStopping;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The TaskManager is responsible for managing tasks inside the Trogdor coordinator.
 *
 * The task manager has a single thread, managed by the executor.  We start, stop,
 * and handle state changes to tasks by adding requests to the executor queue.
 * Because the executor is single threaded, no locks are needed when accessing
 * TaskManager data structures.
 *
 * The TaskManager maintains a state machine for each task.  Tasks begin in the
 * PENDING state, waiting for their designated start time to arrive.
 * When their time arrives, they transition to the RUNNING state.  In this state,
 * the NodeManager will start them, and monitor them.
 *
 * The TaskManager does not handle communication with the agents.  This is handled
 * by the NodeManagers.  There is one NodeManager per node being managed.
 * See {org.apache.kafka.trogdor.coordinator.NodeManager} for details.
 */
public final class TaskManager {
    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    /**
     * The platform.
     */
    private final Platform platform;

    /**
     * The scheduler to use for this coordinator.
     */
    private final Scheduler scheduler;

    /**
     * The clock to use for this coordinator.
     */
    private final Time time;

    /**
     * A map of task IDs to Task objects.
     */
    private final Map<String, ManagedTask> tasks;

    /**
     * The executor used for handling Task state changes.
     */
    private final ScheduledExecutorService executor;

    /**
     * Maps node names to node managers.
     */
    private final Map<String, NodeManager> nodeManagers;

    /**
     * True if the TaskManager is shut down.
     */
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    TaskManager(Platform platform, Scheduler scheduler) {
        this.platform = platform;
        this.scheduler = scheduler;
        this.time = scheduler.time();
        this.tasks = new HashMap<>();
        this.executor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("TaskManagerStateThread", false));
        this.nodeManagers = new HashMap<>();
        for (Node node : platform.topology().nodes().values()) {
            if (Node.Util.getTrogdorAgentPort(node) > 0) {
                this.nodeManagers.put(node.name(), new NodeManager(node, this));
            }
        }
        log.info("Created TaskManager for agent(s) on: {}",
            Utils.join(nodeManagers.keySet(), ", "));
    }

    enum ManagedTaskState {
        PENDING,
        RUNNING,
        STOPPING,
        DONE;
    }

    class ManagedTask {
        /**
         * The task id.
         */
        final private String id;

        /**
         * The task specification.
         */
        final private TaskSpec spec;

        /**
         * The task controller.
         */
        final private TaskController controller;

        /**
         * The task state.
         */
        private ManagedTaskState state;

        /**
         * The time when the task was started, or -1 if the task has not been started.
         */
        private long startedMs = -1;

        /**
         * The time when the task was finished, or -1 if the task has not been finished.
         */
        private long doneMs = -1;

        /**
         * True if the task was cancelled by a stop request.
         */
        boolean cancelled = false;

        /**
         * If there is a task start scheduled, this is a future which can
         * be used to cancel it.
         */
        private Future<?> startFuture = null;

        /**
         * The name of the worker nodes involved with this task.
         * Null if the task is not running.
         */
        private Set<String> workers = null;

        /**
         * The names of the worker nodes which are still running this task.
         * Null if the task is not running.
         */
        private Set<String> activeWorkers = null;

        /**
         * If this is non-empty, a message describing how this task failed.
         */
        private String error = "";

        ManagedTask(String id, TaskSpec spec, TaskController controller, ManagedTaskState state) {
            this.id = id;
            this.spec = spec;
            this.controller = controller;
            this.state = state;
        }

        void clearStartFuture() {
            if (startFuture != null) {
                startFuture.cancel(false);
                startFuture = null;
            }
        }

        long startDelayMs(long now) {
            if (now > spec.startMs()) {
                return 0;
            }
            return spec.startMs() - now;
        }

        TreeSet<String> findNodeNames() {
            Set<String> nodeNames = controller.targetNodes(platform.topology());
            TreeSet<String> validNodeNames = new TreeSet<>();
            TreeSet<String> nonExistentNodeNames = new TreeSet<>();
            for (String nodeName : nodeNames) {
                if (nodeManagers.containsKey(nodeName)) {
                    validNodeNames.add(nodeName);
                } else {
                    nonExistentNodeNames.add(nodeName);
                }
            }
            if (!nonExistentNodeNames.isEmpty()) {
                throw new KafkaException("Unknown node names: " +
                        Utils.join(nonExistentNodeNames, ", "));
            }
            if (validNodeNames.isEmpty()) {
                throw new KafkaException("No node names specified.");
            }
            return validNodeNames;
        }

        void maybeSetError(String newError) {
            if (error.isEmpty()) {
                error = newError;
            }
        }

        TaskState taskState() {
            switch (state) {
                case PENDING:
                    return new TaskPending(spec);
                case RUNNING:
                    return new TaskRunning(spec, startedMs);
                case STOPPING:
                    return new TaskStopping(spec, startedMs);
                case DONE:
                    return new TaskDone(spec, startedMs, doneMs, error, cancelled);
            }
            throw new RuntimeException("unreachable");
        }
    }

    /**
     * Create a task.
     *
     * @param id                    The ID of the task to create.
     * @param spec                  The specification of the task to create.
     *
     * @return                      The specification of the task with the given ID.
     *                              Note that if there was already a task with the given ID,
     *                              this may be different from the specification that was
     *                              requested.
     */
    public TaskSpec createTask(final String id, TaskSpec spec)
            throws ExecutionException, InterruptedException {
        final TaskSpec existingSpec = executor.submit(new CreateTask(id, spec)).get();
        if (existingSpec != null) {
            log.info("Ignoring request to create task {}, because there is already " +
                "a task with that id.", id);
            return existingSpec;
        }
        return spec;
    }

    /**
     * Handles a request to create a new task.  Processed by the state change thread.
     */
    class CreateTask implements Callable<TaskSpec> {
        private final String id;
        private final TaskSpec spec;

        CreateTask(String id, TaskSpec spec) {
            this.id = id;
            this.spec = spec;
        }

        @Override
        public TaskSpec call() throws Exception {
            ManagedTask task = tasks.get(id);
            if (task != null) {
                log.info("Task ID {} is already in use.", id);
                return task.spec;
            }
            TaskController controller = null;
            String failure = null;
            try {
                controller = spec.newController(id);
            } catch (Throwable t) {
                failure = "Failed to create TaskController: " + t.getMessage();
            }
            if (failure != null) {
                log.info("Failed to create a new task {} with spec {}: {}",
                    id, spec, failure);
                task = new ManagedTask(id, spec, null, ManagedTaskState.DONE);
                task.doneMs = time.milliseconds();
                task.maybeSetError(failure);
                tasks.put(id, task);
                return null;
            }
            task = new ManagedTask(id, spec, controller, ManagedTaskState.PENDING);
            tasks.put(id, task);
            long delayMs = task.startDelayMs(time.milliseconds());
            task.startFuture = scheduler.schedule(executor, new RunTask(task), delayMs);
            log.info("Created a new task {} with spec {}, scheduled to start {} ms from now.",
                    id, spec, delayMs);
            return null;
        }
    }

    /**
     * Handles starting a task.  Processed by the state change thread.
     */
    class RunTask implements Callable<Void> {
        private final ManagedTask task;

        RunTask(ManagedTask task) {
            this.task = task;
        }

        @Override
        public Void call() throws Exception {
            task.clearStartFuture();
            if (task.state != ManagedTaskState.PENDING) {
                log.info("Can't start task {}, because it is already in state {}.",
                    task.id, task.state);
                return null;
            }
            TreeSet<String> nodeNames;
            try {
                nodeNames = task.findNodeNames();
            } catch (Exception e) {
                log.error("Unable to find nodes for task {}", task.id, e);
                task.doneMs = time.milliseconds();
                task.state = ManagedTaskState.DONE;
                task.maybeSetError("Unable to find nodes for task: " + e.getMessage());
                return null;
            }
            log.info("Running task {} on node(s): {}", task.id, Utils.join(nodeNames, ", "));
            task.state = ManagedTaskState.RUNNING;
            task.startedMs = time.milliseconds();
            task.workers = nodeNames;
            task.activeWorkers = new HashSet<>();
            for (String workerName : task.workers) {
                task.activeWorkers.add(workerName);
                nodeManagers.get(workerName).createWorker(task.id, task.spec);
            }
            return null;
        }
    }

    /**
     * Stop a task.
     *
     * @param id                    The ID of the task to stop.
     * @return                      The specification of the task which was stopped, or null if there
     *                              was no task found with the given ID.
     */
    public TaskSpec stopTask(final String id) throws ExecutionException, InterruptedException {
        final TaskSpec spec = executor.submit(new CancelTask(id)).get();
        return spec;
    }

    /**
     * Handles cancelling a task.  Processed by the state change thread.
     */
    class CancelTask implements Callable<TaskSpec> {
        private final String id;

        CancelTask(String id) {
            this.id = id;
        }

        @Override
        public TaskSpec call() throws Exception {
            ManagedTask task = tasks.get(id);
            if (task == null) {
                log.info("Can't cancel non-existent task {}.", id);
                return null;
            }
            switch (task.state) {
                case PENDING:
                    task.cancelled = true;
                    task.clearStartFuture();
                    task.doneMs = time.milliseconds();
                    task.state = ManagedTaskState.DONE;
                    log.info("Stopped pending task {}.", id);
                    break;
                case RUNNING:
                    task.cancelled = true;
                    if (task.activeWorkers.size() == 0) {
                        log.info("Task {} is now complete with error: {}", id, task.error);
                        task.doneMs = time.milliseconds();
                        task.state = ManagedTaskState.DONE;
                    } else {
                        for (String workerName : task.activeWorkers) {
                            nodeManagers.get(workerName).stopWorker(id);
                        }
                        log.info("Cancelling task {} on worker(s): {}", id, Utils.join(task.activeWorkers, ", "));
                        task.state = ManagedTaskState.STOPPING;
                    }
                    break;
                case STOPPING:
                    log.info("Can't cancel task {} because it is already stopping.", id);
                    break;
                case DONE:
                    log.info("Can't cancel task {} because it is already done.", id);
                    break;
            }
            return task.spec;
        }
    }

    /**
     * A callback NodeManager makes to indicate that a worker has completed.
     * The task will transition to DONE once all workers are done.
     *
     * @param nodeName      The node name.
     * @param id            The worker name.
     * @param error         An empty string if there is no error, or an error string.
     */
    public void handleWorkerCompletion(String nodeName, String id, String error) {
        executor.submit(new HandleWorkerCompletion(nodeName, id, error));
    }

    class HandleWorkerCompletion implements Callable<Void> {
        private final String nodeName;
        private final String id;
        private final String error;

        HandleWorkerCompletion(String nodeName, String id, String error) {
            this.nodeName = nodeName;
            this.id = id;
            this.error = error;
        }

        @Override
        public Void call() throws Exception {
            ManagedTask task = tasks.get(id);
            if (task == null) {
                log.error("Can't handle completion of unknown worker {} on node {}",
                    id, nodeName);
                return null;
            }
            if ((task.state == ManagedTaskState.PENDING) || (task.state == ManagedTaskState.DONE)) {
                log.error("Task {} got unexpected worker completion from {} while " +
                    "in {} state.", id, nodeName, task.state);
                return null;
            }
            boolean broadcastStop = false;
            if (task.state == ManagedTaskState.RUNNING) {
                task.state = ManagedTaskState.STOPPING;
                broadcastStop = true;
            }
            task.maybeSetError(error);
            task.activeWorkers.remove(nodeName);
            if (task.activeWorkers.size() == 0) {
                task.doneMs = time.milliseconds();
                task.state = ManagedTaskState.DONE;
                log.info("Task {} is now complete on {} with error: {}", id,
                    Utils.join(task.workers, ", "),
                    task.error.isEmpty() ? "(none)" : task.error);
            } else if (broadcastStop) {
                log.info("Node {} stopped.  Stopping task {} on worker(s): {}",
                    id, Utils.join(task.activeWorkers, ", "));
                for (String workerName : task.activeWorkers) {
                    nodeManagers.get(workerName).stopWorker(id);
                }
            }
            return null;
        }
    }

    /**
     * Get information about the tasks being managed.
     */
    public TasksResponse tasks(TasksRequest request) throws ExecutionException, InterruptedException {
        return executor.submit(new GetTasksResponse(request)).get();
    }

    class GetTasksResponse implements Callable<TasksResponse> {
        private final TasksRequest request;

        GetTasksResponse(TasksRequest request) {
            this.request = request;
        }

        @Override
        public TasksResponse call() throws Exception {
            TreeMap<String, TaskState> states = new TreeMap<>();
            for (ManagedTask task : tasks.values()) {
                if (request.matches(task.id, task.startedMs, task.doneMs)) {
                    states.put(task.id, task.taskState());
                }
            }
            return new TasksResponse(states);
        }
    }

    /**
     * Initiate shutdown, but do not wait for it to complete.
     */
    public void beginShutdown(boolean stopAgents) throws ExecutionException, InterruptedException {
        if (shutdown.compareAndSet(false, true)) {
            executor.submit(new Shutdown(stopAgents));
        }
    }

    /**
     * Wait for shutdown to complete.  May be called prior to beginShutdown.
     */
    public void waitForShutdown() throws ExecutionException, InterruptedException {
        while (!executor.awaitTermination(1, TimeUnit.DAYS)) { }
    }

    class Shutdown implements Callable<Void> {
        private final boolean stopAgents;

        Shutdown(boolean stopAgents) {
            this.stopAgents = stopAgents;
        }

        @Override
        public Void call() throws Exception {
            log.info("Shutting down TaskManager{}.", stopAgents ? " and agents" : "");
            for (NodeManager nodeManager : nodeManagers.values()) {
                nodeManager.beginShutdown(stopAgents);
            }
            for (NodeManager nodeManager : nodeManagers.values()) {
                nodeManager.waitForShutdown();
            }
            executor.shutdown();
            return null;
        }
    }
};
