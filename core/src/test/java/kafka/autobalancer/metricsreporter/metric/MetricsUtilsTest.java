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

import kafka.autobalancer.common.types.RawMetricTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class MetricsUtilsTest {

    @Test
    public void testSanityCheckTopicPartitionMetricsCompleteness() {
        TopicPartitionMetrics metrics = new TopicPartitionMetrics(System.currentTimeMillis(), 1, "", "testTopic", 0);
        metrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        Assertions.assertFalse(MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics));
        metrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        Assertions.assertFalse(MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics));
        metrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics));
    }
}
