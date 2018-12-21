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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.TreeMap;

public class ToolsUtils {

    /**
     * print out the metrics in alphabetical order
     * @param metrics   the metrics to be printed out
     */
    public static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
        if (metrics != null && !metrics.isEmpty()) {
            int maxLengthOfDisplayName = 0;
            TreeMap<String, Object> sortedMetrics = new TreeMap<>();
            for (Metric metric : metrics.values()) {
                MetricName mName = metric.metricName();
                String mergedName = mName.group() + ":" + mName.name() + ":" + mName.tags();
                maxLengthOfDisplayName = maxLengthOfDisplayName < mergedName.length() ? mergedName.length() : maxLengthOfDisplayName;
                sortedMetrics.put(mergedName, metric.metricValue());
            }
            String doubleOutputFormat = "%-" + maxLengthOfDisplayName + "s : %.3f";
            String defaultOutputFormat = "%-" + maxLengthOfDisplayName + "s : %s";
            System.out.println(String.format("\n%-" + maxLengthOfDisplayName + "s   %s", "Metric Name", "Value"));

            for (Map.Entry<String, Object> entry : sortedMetrics.entrySet()) {
                String outputFormat;
                if (entry.getValue() instanceof Double)
                    outputFormat = doubleOutputFormat;
                else
                    outputFormat = defaultOutputFormat;
                System.out.println(String.format(outputFormat, entry.getKey(), entry.getValue()));
            }
        }
    }
}
