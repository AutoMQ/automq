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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;

/**
 * Thrown when a sensor records a value that causes a metric to go outside the bounds configured as its quota
 */
public class QuotaViolationException extends KafkaException {

    private static final long serialVersionUID = 1L;
    private final MetricName metricName;
    private final double value;
    private final double bound;

    public QuotaViolationException(MetricName metricName, double value, double bound) {
        this.metricName = metricName;
        this.value = value;
        this.bound = bound;
    }

    public MetricName metricName() {
        return metricName;
    }

    public double value() {
        return value;
    }

    public double bound() {
        return bound;
    }

    @Override
    public String toString() {
        return getClass().getName()
                + ": '"
                + metricName
                + "' violated quota. Actual: "
                + value
                + ", Threshold: "
                + bound;
    }

    /* avoid the expensive and stack trace for quota violation exceptions */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
