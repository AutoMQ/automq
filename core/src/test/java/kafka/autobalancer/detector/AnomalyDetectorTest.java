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

package kafka.autobalancer.detector;

import com.automq.stream.s3.metrics.TimerUtil;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.RawMetricType;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.goals.NetworkInUsageDistributionGoal;
import kafka.autobalancer.goals.NetworkOutUsageDistributionGoal;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AnomalyDetectorTest {

    @Test
    public void testSchedulingTimeCost() {
        ClusterModel clusterModel = new ClusterModel();

        int brokerNum = 20;
        for (int i = 0; i < brokerNum; i++) {
            clusterModel.registerBroker(i, "");
        }
        int topicNum = 5000;
        int totalPartitionNum = 100000;
        int partitionNumPerTopic = totalPartitionNum / topicNum;
        Random r = new Random();
        int[] partitionNums = generatePartitionDist(totalPartitionNum, brokerNum);
        Assertions.assertEquals(totalPartitionNum, Arrays.stream(partitionNums).sum());
        int currPartitionNum = 0;
        int brokerIndex = 0;
        for (int i = 0; i < topicNum; i++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic-" + i;
            clusterModel.createTopic(topicId, topicName);
            for (int j = 0; j < partitionNumPerTopic; j++) {
                clusterModel.createPartition(topicId, j, brokerIndex);
                Map<RawMetricType, Double> metrics = generateRandomMetrics(r);
                clusterModel.updateTopicPartitionMetrics(brokerIndex, new TopicPartition(topicName, j), metrics, System.currentTimeMillis());
                currPartitionNum++;
                if (currPartitionNum >= partitionNums[brokerIndex]) {
                    brokerIndex++;
                    currPartitionNum = 0;
                }
            }
        }

        Map<String, ?> configs = new AutoBalancerControllerConfig(Collections.emptyMap(), false).originals();
        Goal goal0 = new NetworkInUsageDistributionGoal();
        Goal goal1 = new NetworkOutUsageDistributionGoal();
        goal0.configure(configs);
        goal1.configure(configs);

        List<Action> actionList = new ArrayList<>();
        AnomalyDetector detector = new AnomalyDetectorBuilder()
                .clusterModel(clusterModel)
                .addGoal(goal0)
                .addGoal(goal1)
                .executor(new ActionExecutorService() {
                    @Override
                    public void start() {

                    }

                    @Override
                    public void shutdown() {

                    }

                    @Override
                    public void execute(Action action) {
                        actionList.add(action);
                    }

                    @Override
                    public void execute(List<Action> actions) {
                        actionList.addAll(actions);
                    }
                })
                .build();

        TimerUtil timerUtil = new TimerUtil();
        detector.resume();
        detector.detect();
        System.out.printf("Detect cost: %d ms, %d actions detected%n", timerUtil.elapsedAs(TimeUnit.MILLISECONDS), actionList.size());
        Assertions.assertFalse(actionList.isEmpty());

        ClusterModelSnapshot snapshot = clusterModel.snapshot();

        double[] loadsBefore = snapshot.brokers().stream().map(b -> b.load(Resource.NW_IN)).mapToDouble(Double::doubleValue).toArray();
        double meanBefore = Arrays.stream(loadsBefore).sum() / loadsBefore.length;
        double stdDevBefore = calculateStdDev(meanBefore, loadsBefore);
        for (Action action : actionList) {
            snapshot.applyAction(action);
        }
        double[] loadsAfter = snapshot.brokers().stream().map(b -> b.load(Resource.NW_IN)).mapToDouble(Double::doubleValue).toArray();
        double meanAfter = Arrays.stream(loadsBefore).sum() / loadsBefore.length;
        double stdDevAfter = calculateStdDev(meanAfter, loadsAfter);
        Assertions.assertEquals(meanBefore, meanAfter);
        Assertions.assertTrue(stdDevAfter < stdDevBefore);
        System.out.printf("mean: %f, stdDev before: %f (%.2f%%), after: %f (%.2f%%), %n", meanAfter, stdDevBefore,
                100.0 * stdDevBefore / meanBefore, stdDevAfter, 100.0 * stdDevAfter / meanAfter);
    }

    private double calculateStdDev(double mean, double[] values) {
        double sum = Arrays.stream(values).map(v -> Math.pow(v - mean, 2)).sum();
        return Math.sqrt(sum / values.length);
    }

    private int[] generatePartitionDist(int totalPartitionNum, int brokerNum) {
        int[] partitionNums = new int[brokerNum];
        PoissonDistribution poissonDistribution = new PoissonDistribution(10);
        int[] samples = poissonDistribution.sample(brokerNum);
        int sum = Arrays.stream(samples).sum();
        for (int i = 0; i < brokerNum; i++) {
            partitionNums[i] = (int) (samples[i] * 1.0 / sum * totalPartitionNum);
        }
        int partitionSum = Arrays.stream(partitionNums).sum();
        partitionNums[0] += totalPartitionNum - partitionSum;
        return partitionNums;
    }

    private Map<RawMetricType, Double> generateRandomMetrics(Random r) {
        Map<RawMetricType, Double> metrics = new HashMap<>();
        metrics.put(RawMetricType.TOPIC_PARTITION_BYTES_OUT, r.nextDouble(0, 1024 * 1024));
        metrics.put(RawMetricType.TOPIC_PARTITION_BYTES_IN, r.nextDouble(0, 1024 * 1024));
        metrics.put(RawMetricType.PARTITION_SIZE, 0.0);
        return metrics;
    }
}
