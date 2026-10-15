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
package org.apache.kafka.connect.tools;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

/**
 * Exposes task metrics in a previously unknown JMX domain for automatic-discovery system tests.
 */
public class JmxMetricsSourceTask extends VerifiableSourceTask {
    private final TaskMetrics metrics = new TaskMetrics();
    private ObjectName objectName;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        Hashtable<String, String> properties = new Hashtable<>();
        properties.put("type", "TaskMetrics");
        properties.put("connector", props.get(NAME_CONFIG));
        properties.put("task", props.get(ID_CONFIG));
        try {
            objectName = new ObjectName("automq.connect.test", properties);
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                new StandardMBean(metrics, TaskMetricsMBean.class), objectName);
        } catch (JMException e) {
            super.stop();
            throw new ConnectException("Failed to register system-test task metrics", e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = super.poll();
        metrics.recordsPolled.addAndGet(records.size());
        return records;
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            if (objectName != null) {
                try {
                    ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectName);
                } catch (InstanceNotFoundException ignored) {
                    // The task may already have unregistered its metrics during shutdown.
                } catch (JMException e) {
                    throw new ConnectException("Failed to unregister system-test task metrics", e);
                }
                objectName = null;
            }
        }
    }

    /** The portable JMX contract exposed by this test task. */
    public interface TaskMetricsMBean {
        /** Returns the number of records polled by this task. */
        long getRecordsPolled();

        /** Returns whether this registered task is connected. */
        boolean isConnected();
    }

    /** Numeric and Boolean attributes deliberately independent of Kafka Metrics. */
    public static class TaskMetrics implements TaskMetricsMBean {
        private final AtomicLong recordsPolled = new AtomicLong();

        @Override
        public long getRecordsPolled() {
            return recordsPolled.get();
        }

        @Override
        public boolean isConnected() {
            return true;
        }
    }
}
