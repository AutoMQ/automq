/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.autobalancer.detector;

import kafka.autobalancer.services.AbstractResumableService;

import org.apache.kafka.automq.ControllerState;
import org.apache.kafka.common.Reconfigurable;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.utils.LogContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

public abstract class AbstractAnomalyDetector extends AbstractResumableService implements Reconfigurable {
    private static final AttributeKey<String> LABEL_NODE_ID = AttributeKey.stringKey("node_id");
    private static final AtomicReference<AbstractAnomalyDetector> ACTIVE_DETECTOR = new AtomicReference<>();
    private static final Metrics.LongGaugeBundle SLOW_BROKER = Metrics.instance()
        .longGauge("kafka_stream_slow_broker_count", "The metrics to indicate whether the broker is slow or not", "");

    static {
        SLOW_BROKER.register(MetricsLevel.INFO, Attributes.empty(), AbstractAnomalyDetector::recordSlowBrokers);
    }

    protected volatile Map<Integer, Boolean> slowBrokers = Collections.emptyMap();

    public AbstractAnomalyDetector(LogContext logContext) {
        super(logContext);
        ACTIVE_DETECTOR.set(this);
    }

    private static void recordSlowBrokers(Metrics.LongGaugeBundle.ObservableLong measurement) {
        AbstractAnomalyDetector detector = ACTIVE_DETECTOR.get();
        if (detector == null || !ControllerState.isActive()) {
            return;
        }
        detector.slowBrokers.forEach((brokerId, isSlow) ->
            measurement.record(isSlow ? 1 : 0, Attributes.of(LABEL_NODE_ID, String.valueOf(brokerId))));
    }

}
