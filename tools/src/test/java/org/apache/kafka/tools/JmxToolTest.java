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
package org.apache.kafka.tools;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JmxToolTest {
    private final ToolsTestUtils.MockExitProcedure exitProcedure = new ToolsTestUtils.MockExitProcedure();

    private static JMXConnectorServer jmxAgent;
    private static String jmxUrl;

    @BeforeAll
    public static void beforeAll() throws Exception {
        int port = findRandomOpenPortOnAllLocalInterfaces();
        jmxAgent = startJmxAgent(port);
        jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://:%d/jmxrmi", port);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        jmxAgent.stop();
    }

    @BeforeEach
    public void beforeEach() {
        Exit.setExitProcedure(exitProcedure);
    }

    @AfterEach
    public void afterEach() {
        Exit.resetExitProcedure();
    }

    @Test
    public void kafkaVersion() {
        String out = executeAndGetOut("--version");
        assertNormalExit();
        assertTrue(out.contains(AppInfoParser.getVersion()));
    }

    @Test
    public void unrecognizedOption() {
        String err = executeAndGetErr("--foo");
        assertCommandFailure();
        assertTrue(err.contains("UnrecognizedOptionException"));
        assertTrue(err.contains("foo"));
    }

    @Test
    public void missingRequired() {
        String err = executeAndGetErr("--reporting-interval");
        assertCommandFailure();
        assertTrue(err.contains("OptionMissingRequiredArgumentException"));
        assertTrue(err.contains("reporting-interval"));
    }

    @Test
    public void malformedURL() {
        String err = executeAndGetErr("--jmx-url", "localhost:9999");
        assertCommandFailure();
        assertTrue(err.contains("MalformedURLException"));
    }

    @Test
    public void helpOptions() {
        String[] expectedOptions = new String[]{
            "--attributes", "--date-format", "--help", "--jmx-auth-prop",
            "--jmx-ssl-enable", "--jmx-url", "--object-name", "--one-time",
            "--report-format", "--reporting-interval", "--version", "--wait"
        };
        String err = executeAndGetErr("--help");
        assertCommandFailure();
        for (String option : expectedOptions) {
            assertTrue(err.contains(option), option);
        }
    }

    @Test
    public void csvFormat() {
        String[] args = new String[]{
            "--jmx-url", jmxUrl,
            "--object-name", "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
            "--report-format", "csv",
            "--one-time"
        };
        String out = executeAndGetOut(args);
        Arrays.stream(out.split("\\r?\\n")).forEach(line -> {
            assertTrue(line.matches("([a-zA-Z0-9=:,.]+),\"([ -~]+)\""), line);
        });
    }

    @Test
    public void tsvFormat() {
        String[] args = new String[]{
            "--jmx-url", jmxUrl,
            "--object-name", "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
            "--report-format", "tsv",
            "--one-time"
        };
        String out = executeAndGetOut(args);
        Arrays.stream(out.split("\\r?\\n")).forEach(line -> {
            assertTrue(line.matches("([a-zA-Z0-9=:,.]+)\\t([ -~]+)"), line);
        });
    }

    @Test
    public void allMetrics() {
        String[] args = new String[]{
            "--jmx-url", jmxUrl,
            "--report-format", "csv",
            "--reporting-interval", "-1"
        };
        String out = executeAndGetOut(args);
        assertNormalExit();

        Map<String, String> csv = parseCsv(out);
        assertTrue(csv.size() > 0);
    }

    @Test
    public void filteredMetrics() {
        String[] args = new String[]{
            "--jmx-url", jmxUrl,
            "--object-name", "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
            "--attributes", "FifteenMinuteRate,FiveMinuteRate",
            "--report-format", "csv",
            "--one-time"
        };
        String out = executeAndGetOut(args);
        assertNormalExit();

        Map<String, String> csv = parseCsv(out);
        assertEquals("1.0", csv.get("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:FifteenMinuteRate"));
        assertEquals("3.0", csv.get("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:FiveMinuteRate"));
    }

    @Test
    public void dateFormat() {
        String dateFormat = "yyyyMMdd-hh:mm:ss";
        String[] args = new String[]{
            "--jmx-url", jmxUrl,
            "--date-format", dateFormat,
            "--report-format", "csv",
            "--one-time"
        };
        String out = executeAndGetOut(args);
        assertNormalExit();

        Map<String, String> csv = parseCsv(out);
        assertTrue(validDateFormat(dateFormat, csv.get("time")));
    }

    private static JMXConnectorServer startJmxAgent(int port) throws Exception {
        LocateRegistry.createRegistry(port);
        Map<String, Object> env = new HashMap<>();
        env.put("com.sun.management.jmxremote.authenticate", "false");
        env.put("com.sun.management.jmxremote.ssl", "false");
        JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://:%d/jmxrmi", port));
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        server.registerMBean(new Metrics(),
            new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"));
        JMXConnectorServer agent = JMXConnectorServerFactory.newJMXConnectorServer(url, env, server);
        agent.start();
        return agent;
    }

    private static int findRandomOpenPortOnAllLocalInterfaces() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private String executeAndGetOut(String... args) {
        return execute(args, false);
    }

    private String executeAndGetErr(String... args) {
        return execute(args, true);
    }

    private String execute(String[] args, boolean err) {
        Runnable runnable = () -> {
            try {
                JmxTool.main(args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return err ? ToolsTestUtils.captureStandardErr(runnable)
                    : ToolsTestUtils.captureStandardOut(runnable);
    }

    private void assertNormalExit() {
        assertTrue(exitProcedure.hasExited());
        assertEquals(0, exitProcedure.statusCode());
    }

    private void assertCommandFailure() {
        assertTrue(exitProcedure.hasExited());
        assertEquals(1, exitProcedure.statusCode());
    }

    private Map<String, String> parseCsv(String value) {
        Map<String, String> result = new HashMap<>();
        Arrays.stream(value.split("\\r?\\n")).forEach(line -> {
            String[] cells = line.split(",\"");
            if (cells.length == 2) {
                result.put(cells[0], cells[1].replaceAll("\"", ""));
            }
        });
        return result;
    }

    private boolean validDateFormat(String format, String value) {
        DateFormat formatter = new SimpleDateFormat(format);
        formatter.setLenient(false);
        try {
            formatter.parse(value);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public interface MetricsMBean {
        double getFifteenMinuteRate();
        double getFiveMinuteRate();
    }

    public static class Metrics implements MetricsMBean {
        @Override
        public double getFifteenMinuteRate() {
            return 1.0;
        }

        @Override
        public double getFiveMinuteRate() {
            return 3.0;
        }
    }
}
