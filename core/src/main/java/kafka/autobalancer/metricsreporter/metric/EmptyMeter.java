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

package kafka.autobalancer.metricsreporter.metric;

import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;

import java.util.concurrent.TimeUnit;

public class EmptyMeter implements Metered {
    @Override
    public TimeUnit rateUnit() {
        return null;
    }

    @Override
    public String eventType() {
        return null;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public double fifteenMinuteRate() {
        return 0;
    }

    @Override
    public double fiveMinuteRate() {
        return 0;
    }

    @Override
    public double meanRate() {
        return 0;
    }

    @Override
    public double oneMinuteRate() {
        return 0;
    }

    @Override
    public <T> void processWith(MetricProcessor<T> processor, MetricName name, T context) throws Exception {
        processor.processMeter(name, this, context);
    }
}
