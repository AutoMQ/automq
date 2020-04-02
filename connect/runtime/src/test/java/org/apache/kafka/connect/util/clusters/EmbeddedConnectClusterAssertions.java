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
package org.apache.kafka.connect.util.clusters;

import java.util.Map;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.apache.kafka.test.TestUtils.waitForCondition;

/**
 * A set of common assertions that can be applied to a Connect cluster during integration testing
 */
public class EmbeddedConnectClusterAssertions {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectClusterAssertions.class);
    public static final long WORKER_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
    public static final long VALIDATION_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long CONNECTOR_SETUP_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long CONNECT_INTERNAL_TOPIC_UPDATES_DURATION_MS = TimeUnit.SECONDS.toMillis(60);

    private final EmbeddedConnectCluster connect;

    EmbeddedConnectClusterAssertions(EmbeddedConnectCluster connect) {
        this.connect = connect;
    }

    /**
     * Assert that at least the requested number of workers are up and running.
     *
     * @param numWorkers the number of online workers
     */
    public void assertAtLeastNumWorkersAreUp(int numWorkers, String detailMessage) throws InterruptedException {
        try {
            waitForCondition(
                () -> checkWorkersUp(numWorkers, (actual, expected) -> actual >= expected).orElse(false),
                WORKER_SETUP_DURATION_MS,
                "Didn't meet the minimum requested number of online workers: " + numWorkers);
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Assert that at least the requested number of workers are up and running.
     *
     * @param numWorkers the number of online workers
     */
    public void assertExactlyNumWorkersAreUp(int numWorkers, String detailMessage) throws InterruptedException {
        try {
            waitForCondition(
                () -> checkWorkersUp(numWorkers, (actual, expected) -> actual == expected).orElse(false),
                WORKER_SETUP_DURATION_MS,
                "Didn't meet the exact requested number of online workers: " + numWorkers);
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Confirm that the requested number of workers are up and running.
     *
     * @param numWorkers the number of online workers
     * @return true if at least {@code numWorkers} are up; false otherwise
     */
    protected Optional<Boolean> checkWorkersUp(int numWorkers, BiFunction<Integer, Integer, Boolean> comp) {
        try {
            int numUp = connect.activeWorkers().size();
            return Optional.of(comp.apply(numUp, numWorkers));
        } catch (Exception e) {
            log.error("Could not check active workers.", e);
            return Optional.empty();
        }
    }

    /**
     * Assert that the required number of errors are produced by a connector config validation.
     *
     * @param connectorClass the class of the connector to validate
     * @param connConfig     the intended configuration
     * @param numErrors      the number of errors expected
     */
    public void assertExactlyNumErrorsOnConnectorConfigValidation(String connectorClass, Map<String, String> connConfig,
        int numErrors, String detailMessage) throws InterruptedException {
        try {
            waitForCondition(
                () -> checkValidationErrors(
                    connectorClass,
                    connConfig,
                    numErrors,
                    (actual, expected) -> actual == expected
                ).orElse(false),
                VALIDATION_DURATION_MS,
                "Didn't meet the exact requested number of validation errors: " + numErrors);
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Confirm that the requested number of errors are produced by {@link EmbeddedConnectCluster#validateConnectorConfig}.
     *
     * @param connectorClass the class of the connector to validate
     * @param connConfig     the intended configuration
     * @param numErrors      the number of errors expected
     * @return true if exactly {@code numErrors} are produced by the validation; false otherwise
     */
    protected Optional<Boolean> checkValidationErrors(String connectorClass, Map<String, String> connConfig,
        int numErrors, BiFunction<Integer, Integer, Boolean> comp) {
        try {
            int numErrorsProduced = connect.validateConnectorConfig(connectorClass, connConfig).errorCount();
            return Optional.of(comp.apply(numErrorsProduced, numErrors));
        } catch (Exception e) {
            log.error("Could not check config validation error count.", e);
            return Optional.empty();
        }
    }

    /**
     * Assert that a connector is running with at least the given number of tasks all in running state
     *
     * @param connectorName the connector name
     * @param numTasks the number of tasks
     * @param detailMessage
     * @throws InterruptedException
     */
    public void assertConnectorAndAtLeastNumTasksAreRunning(String connectorName, int numTasks, String detailMessage)
            throws InterruptedException {
        try {
            waitForCondition(
                () -> checkConnectorState(
                    connectorName,
                    AbstractStatus.State.RUNNING,
                    numTasks,
                    AbstractStatus.State.RUNNING,
                    (actual, expected) -> actual >= expected
                ).orElse(false),
                CONNECTOR_SETUP_DURATION_MS,
                "The connector or at least " + numTasks + " of tasks are not running.");
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Assert that a connector is running with at least the given number of tasks all in running state
     *
     * @param connectorName the connector name
     * @param numTasks the number of tasks
     * @param detailMessage the assertion message
     * @throws InterruptedException
     */
    public void assertConnectorAndExactlyNumTasksAreRunning(String connectorName, int numTasks, String detailMessage)
            throws InterruptedException {
        try {
            waitForCondition(
                () -> checkConnectorState(
                    connectorName,
                    AbstractStatus.State.RUNNING,
                    numTasks,
                    AbstractStatus.State.RUNNING,
                    (actual, expected) -> actual == expected
                ).orElse(false),
                CONNECTOR_SETUP_DURATION_MS,
                "The connector or exactly " + numTasks + " tasks are not running.");
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Assert that a connector is running, that it has a specific number of tasks, and that all of
     * its tasks are in the FAILED state.
     *
     * @param connectorName the connector name
     * @param numTasks the number of tasks
     * @param detailMessage the assertion message
     * @throws InterruptedException
     */
    public void assertConnectorIsRunningAndTasksHaveFailed(String connectorName, int numTasks, String detailMessage)
            throws InterruptedException {
        try {
            waitForCondition(
                () -> checkConnectorState(
                    connectorName,
                    AbstractStatus.State.RUNNING,
                    numTasks,
                    AbstractStatus.State.FAILED,
                    (actual, expected) -> actual >= expected
                ).orElse(false),
                CONNECTOR_SETUP_DURATION_MS,
                "Either the connector is not running or not all the " + numTasks + " tasks have failed.");
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Assert that a connector and its tasks are not running.
     *
     * @param connectorName the connector name
     * @param detailMessage the assertion message
     * @throws InterruptedException
     */
    public void assertConnectorAndTasksAreStopped(String connectorName, String detailMessage)
            throws InterruptedException {
        try {
            waitForCondition(
                () -> checkConnectorAndTasksAreStopped(connectorName),
                CONNECTOR_SETUP_DURATION_MS,
                "At least the connector or one of its tasks is still");
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Check whether the connector or any of its tasks are still in RUNNING state
     *
     * @param connectorName the connector
     * @return true if the connector and all the tasks are not in RUNNING state; false otherwise
     */
    protected boolean checkConnectorAndTasksAreStopped(String connectorName) {
        ConnectorStateInfo info;
        try {
            info = connect.connectorStatus(connectorName);
        } catch (ConnectRestException e) {
            return e.statusCode() == Response.Status.NOT_FOUND.getStatusCode();
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return false;
        }
        if (info == null) {
            return true;
        }
        return !info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                && info.tasks().stream().noneMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
    }

    /**
     * Check whether the given connector state matches the current state of the connector and
     * whether it has at least the given number of tasks, with all the tasks matching the given
     * task state.
     * @param connectorName the connector
     * @param connectorState
     * @param numTasks the expected number of tasks
     * @param tasksState
     * @return true if the connector and tasks are in RUNNING state; false otherwise
     */
    protected Optional<Boolean> checkConnectorState(
            String connectorName,
            AbstractStatus.State connectorState,
            int numTasks,
            AbstractStatus.State tasksState,
            BiFunction<Integer, Integer, Boolean> comp
    ) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            boolean result = info != null
                    && comp.apply(info.tasks().size(), numTasks)
                    && info.connector().state().equals(connectorState.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(tasksState.toString()));
            return Optional.of(result);
        } catch (Exception e) {
            log.error("Could not check connector state info.", e);
            return Optional.empty();
        }
    }

    /**
     * Assert that a connector's set of active topics matches the given collection of topic names.
     *
     * @param connectorName the connector name
     * @param topics a collection of topics to compare against
     * @param detailMessage the assertion message
     * @throws InterruptedException
     */
    public void assertConnectorActiveTopics(String connectorName, Collection<String> topics, String detailMessage) throws InterruptedException {
        try {
            waitForCondition(
                () -> checkConnectorActiveTopics(connectorName, topics).orElse(false),
                CONNECT_INTERNAL_TOPIC_UPDATES_DURATION_MS,
                "Connector active topics don't match the expected collection");
        } catch (AssertionError e) {
            throw new AssertionError(detailMessage, e);
        }
    }

    /**
     * Check whether a connector's set of active topics matches the given collection of topic names.
     *
     * @param connectorName the connector name
     * @param topics a collection of topics to compare against
     * @return true if the connector's active topics matches the given collection; false otherwise
     */
    protected Optional<Boolean> checkConnectorActiveTopics(String connectorName, Collection<String> topics) {
        try {
            ActiveTopicsInfo info = connect.connectorTopics(connectorName);
            boolean result = info != null
                    && topics.size() == info.topics().size()
                    && topics.containsAll(info.topics());
            log.debug("Found connector {} using topics: {}", connectorName, info.topics());
            return Optional.of(result);
        } catch (Exception e) {
            log.error("Could not check connector {} state info.", connectorName, e);
            return Optional.empty();
        }
    }
}
