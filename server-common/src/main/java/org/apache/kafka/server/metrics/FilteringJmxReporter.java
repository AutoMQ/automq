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

package org.apache.kafka.server.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

public class FilteringJmxReporter extends JmxReporter {

    // AutoMQ inject start
    private static final Logger LOGGER = LoggerFactory.getLogger(FilteringJmxReporter.class);
    private final ExecutorService mBeanOperationExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "kafka-yammer-mbean-operator");
        thread.setDaemon(true);
        return thread;
    });
    private final Object submissionLock = new Object();
    private boolean shuttingDown = false;
    // AutoMQ inject end

    private volatile Predicate<MetricName> metricPredicate;

    public FilteringJmxReporter(MetricsRegistry registry, Predicate<MetricName> metricPredicate) {
        super(registry);
        this.metricPredicate = metricPredicate;
    }

    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        // AutoMQ inject start
        reconcileAsync(name);
        // AutoMQ inject end
    }

    // AutoMQ inject start
    @Override
    public void onMetricRemoved(MetricName name) {
        reconcileAsync(name);
    }

    private void reconcileAsync(MetricName name) {
        synchronized (submissionLock) {
            if (!shuttingDown) {
                mBeanOperationExecutor.execute(() -> reconcile(name));
            }
        }
    }

    private void reconcile(MetricName name) {
        try {
            Metric metric = getMetricsRegistry().allMetrics().get(name);
            if (metric != null && metricPredicate.test(name)) {
                super.onMetricAdded(name, metric);
            } else {
                super.onMetricRemoved(name);
            }
        } catch (Throwable ex) {
            LOGGER.warn("Failed to reconcile Yammer metric {} with JMX", name, ex);
        }
    }

    public void updatePredicate(Predicate<MetricName> predicate) {
        this.metricPredicate = predicate;
        runAndWait(() -> getMetricsRegistry().allMetrics().keySet().forEach(this::reconcile));
    }

    @Override
    public void shutdown() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (submissionLock) {
            shuttingDown = true;
            execute(super::shutdown, result);
        }
        result.join();
        mBeanOperationExecutor.shutdown();
    }

    private void runAndWait(Runnable operation) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (submissionLock) {
            if (shuttingDown) {
                return;
            }
            execute(operation, result);
        }
        result.join();
    }

    private void execute(Runnable operation, CompletableFuture<Void> result) {
        mBeanOperationExecutor.execute(() -> {
            try {
                operation.run();
                result.complete(null);
            } catch (Throwable ex) {
                result.completeExceptionally(ex);
            }
        });
    }
    // AutoMQ inject end
}
