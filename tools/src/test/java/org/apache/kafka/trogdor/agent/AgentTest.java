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

package org.apache.kafka.trogdor.agent;

import org.apache.kafka.common.utils.MockScheduler;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.trogdor.basic.BasicNode;
import org.apache.kafka.trogdor.basic.BasicPlatform;
import org.apache.kafka.trogdor.basic.BasicTopology;
import org.apache.kafka.trogdor.common.ExpectedTasks;
import org.apache.kafka.trogdor.common.ExpectedTasks.ExpectedTaskBuilder;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.fault.FilesUnreadableFaultSpec;
import org.apache.kafka.trogdor.fault.Kibosh;
import org.apache.kafka.trogdor.fault.Kibosh.KiboshControlFile;
import org.apache.kafka.trogdor.fault.Kibosh.KiboshFilesUnreadableFaultSpec;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;

import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.CreateWorkerResponse;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.apache.kafka.trogdor.rest.WorkerDone;
import org.apache.kafka.trogdor.rest.WorkerRunning;
import org.apache.kafka.trogdor.task.NoOpTaskSpec;
import org.apache.kafka.trogdor.task.SampleTaskSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class AgentTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private static BasicPlatform createBasicPlatform(Scheduler scheduler) {
        TreeMap<String, Node> nodes = new TreeMap<>();
        HashMap<String, String> config = new HashMap<>();
        nodes.put("node01", new BasicNode("node01", "localhost",
            config, Collections.<String>emptySet()));
        BasicTopology topology = new BasicTopology(nodes);
        return new BasicPlatform("node01", topology,
            scheduler, new BasicPlatform.ShellCommandRunner());
    }

    private Agent createAgent(Scheduler scheduler) {
        JsonRestServer restServer = new JsonRestServer(0);
        AgentRestResource resource = new AgentRestResource();
        restServer.start(resource);
        return new Agent(createBasicPlatform(scheduler), scheduler,
                restServer, resource);
    }

    @Test
    public void testAgentStartShutdown() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentProgrammaticShutdown() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        client.invokeShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentGetStatus() throws Exception {
        Agent agent = createAgent(Scheduler.SYSTEM);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        AgentStatusResponse status = client.status();
        assertEquals(agent.status(), status);
        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentCreateWorkers() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        AgentStatusResponse status = client.status();
        assertEquals(Collections.emptyMap(), status.workers());
        new ExpectedTasks().waitFor(client);

        final NoOpTaskSpec fooSpec = new NoOpTaskSpec(1000, 600000);
        CreateWorkerResponse response = client.createWorker(new CreateWorkerRequest("foo", fooSpec));
        assertEquals(fooSpec.toString(), response.spec().toString());
        new ExpectedTasks().addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning(fooSpec, 0, "")).
                build()).
            waitFor(client);

        final NoOpTaskSpec barSpec = new NoOpTaskSpec(2000, 900000);
        client.createWorker(new CreateWorkerRequest("bar", barSpec));
        client.createWorker(new CreateWorkerRequest("bar", barSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning(fooSpec, 0, "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning(barSpec, 0, "")).
                build()).
            waitFor(client);

        final NoOpTaskSpec bazSpec = new NoOpTaskSpec(1, 450000);
        client.createWorker(new CreateWorkerRequest("baz", bazSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning(fooSpec, 0, "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning(barSpec, 0, "")).
                build()).
            addTask(new ExpectedTaskBuilder("baz").
                workerState(new WorkerRunning(bazSpec, 0, "")).
                build()).
            waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testAgentFinishesTasks() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        final NoOpTaskSpec fooSpec = new NoOpTaskSpec(10, 2);
        client.createWorker(new CreateWorkerRequest("foo", fooSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning(fooSpec, 0, "")).
                build()).
            waitFor(client);

        time.sleep(1);

        final NoOpTaskSpec barSpec = new NoOpTaskSpec(2000, 900000);
        client.createWorker(new CreateWorkerRequest("bar", barSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning(fooSpec, 0, "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning(barSpec, 1, "")).
                build()).
            waitFor(client);

        time.sleep(1);

        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone(fooSpec, 0, 2, "", "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning(barSpec, 1, "")).
                build()).
            waitFor(client);

        time.sleep(5);
        client.stopWorker(new StopWorkerRequest("bar"));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone(fooSpec, 0, 2, "", "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerDone(barSpec, 1, 7, "", "")).
                build()).
            waitFor(client);

        agent.beginShutdown();
        agent.waitForShutdown();
    }

    @Test
    public void testWorkerCompletions() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        SampleTaskSpec fooSpec = new SampleTaskSpec(0, 900000, 1, "");
        client.createWorker(new CreateWorkerRequest("foo", fooSpec));
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerRunning(fooSpec, 0, "")).
                build()).
            waitFor(client);

        SampleTaskSpec barSpec = new SampleTaskSpec(0, 900000, 2, "baz");
        client.createWorker(new CreateWorkerRequest("bar", barSpec));

        time.sleep(1);
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone(fooSpec, 0, 1, "", "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerRunning(barSpec, 0, "")).
                build()).
            waitFor(client);

        time.sleep(1);
        new ExpectedTasks().
            addTask(new ExpectedTaskBuilder("foo").
                workerState(new WorkerDone(fooSpec, 0, 1, "", "")).
                build()).
            addTask(new ExpectedTaskBuilder("bar").
                workerState(new WorkerDone(barSpec, 0, 2, "", "baz")).
                build()).
            waitFor(client);
    }

    private static class MockKibosh implements AutoCloseable {
        private final File tempDir;
        private final Path controlFile;

        MockKibosh() throws IOException {
            tempDir = TestUtils.tempDirectory();
            controlFile = Paths.get(tempDir.toPath().toString(), Kibosh.KIBOSH_CONTROL);
            KiboshControlFile.EMPTY.write(controlFile);
        }

        KiboshControlFile read() throws IOException {
            return KiboshControlFile.read(controlFile);
        }

        @Override
        public void close() throws Exception {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testKiboshFaults() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        MockScheduler scheduler = new MockScheduler(time);
        Agent agent = createAgent(scheduler);
        AgentClient client = new AgentClient.Builder().
            maxTries(10).target("localhost", agent.port()).build();
        new ExpectedTasks().waitFor(client);

        try (MockKibosh mockKibosh = new MockKibosh()) {
            Assert.assertEquals(KiboshControlFile.EMPTY, mockKibosh.read());
            FilesUnreadableFaultSpec fooSpec = new FilesUnreadableFaultSpec(0, 900000,
                Collections.singleton("myAgent"), mockKibosh.tempDir.getPath().toString(), "/foo", 123);
            client.createWorker(new CreateWorkerRequest("foo", fooSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    workerState(new WorkerRunning(fooSpec, 0, "")).
                    build()).
                waitFor(client);
            Assert.assertEquals(new KiboshControlFile(Collections.<Kibosh.KiboshFaultSpec>singletonList(
                new KiboshFilesUnreadableFaultSpec("/foo", 123))), mockKibosh.read());
            FilesUnreadableFaultSpec barSpec = new FilesUnreadableFaultSpec(0, 900000,
                Collections.singleton("myAgent"), mockKibosh.tempDir.getPath().toString(), "/bar", 456);
            client.createWorker(new CreateWorkerRequest("bar", barSpec));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    workerState(new WorkerRunning(fooSpec, 0, "")).build()).
                addTask(new ExpectedTaskBuilder("bar").
                    workerState(new WorkerRunning(barSpec, 0, "")).build()).
                waitFor(client);
            Assert.assertEquals(new KiboshControlFile(new ArrayList<Kibosh.KiboshFaultSpec>() {{
                    add(new KiboshFilesUnreadableFaultSpec("/foo", 123));
                    add(new KiboshFilesUnreadableFaultSpec("/bar", 456));
                }}), mockKibosh.read());
            time.sleep(1);
            client.stopWorker(new StopWorkerRequest("foo"));
            new ExpectedTasks().
                addTask(new ExpectedTaskBuilder("foo").
                    workerState(new WorkerDone(fooSpec, 0, 1, "", "")).build()).
                addTask(new ExpectedTaskBuilder("bar").
                    workerState(new WorkerRunning(barSpec, 0, "")).build()).
                waitFor(client);
            Assert.assertEquals(new KiboshControlFile(Collections.<Kibosh.KiboshFaultSpec>singletonList(
                new KiboshFilesUnreadableFaultSpec("/bar", 456))), mockKibosh.read());
        }
    }
};
