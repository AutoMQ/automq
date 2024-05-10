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
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.goals.AbstractResourceUsageDistributionGoal;
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
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AnomalyDetectorTest {

    @Test
    public void testFilterAndMergeActions() {
        AnomalyDetector anomalyDetector = new AnomalyDetectorBuilder()
                .clusterModel(Mockito.mock(ClusterModel.class))
                .addGoal(Mockito.mock(Goal.class))
                .executor(new ActionExecutorService() {
                    @Override
                    public void start() {

                    }

                    @Override
                    public void shutdown() {

                    }

                    @Override
                    public CompletableFuture<Void> execute(List<Action> actions) {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .build();

        List<Action> actions = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 1, 11),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 0, 1),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 1, 9),
                new Action(ActionType.SWAP, new TopicPartition("topic-2", 3), 9, 11, new TopicPartition("topic-1", 0)),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 3, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 10, 2));
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> anomalyDetector.checkAndMergeActions(actions));

        List<Action> actions1 = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 1, 11),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 0, 1),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 999, 9),
                new Action(ActionType.SWAP, new TopicPartition("topic-2", 3), 9, 3, new TopicPartition("topic-1", 3)),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 3, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 10, 2));
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> anomalyDetector.checkAndMergeActions(actions1));

        List<Action> actions2 = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 1, 11),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 0, 1),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 1, 9),
                new Action(ActionType.SWAP, new TopicPartition("topic-2", 3), 8, 3, new TopicPartition("topic-1", 3)),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 3, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 10, 2));
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> anomalyDetector.checkAndMergeActions(actions2));

        List<Action> actions3 = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 1, 11),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 0, 1),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 1, 9),
                new Action(ActionType.SWAP, new TopicPartition("topic-2", 3), 9, 10, new TopicPartition("topic-1", 0)),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 3, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 10, 2));
        Assertions.assertThrowsExactly(IllegalStateException.class, () -> anomalyDetector.checkAndMergeActions(actions3));

        List<Action> actions4 = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 1, 11),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 0, 1),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 1, 9),
                new Action(ActionType.SWAP, new TopicPartition("topic-2", 3), 9, 11, new TopicPartition("topic-1", 0)),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 11, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 9, 2),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 10, 0));
        List<Action> mergedActions = anomalyDetector.checkAndMergeActions(actions4);
        Assertions.assertEquals(1, mergedActions.size());
        List<Action> expectedActions = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 1, 2));

        Assertions.assertEquals(expectedActions, mergedActions);
    }

    @Test
    public void testGroupActions() {
        AnomalyDetector anomalyDetector = new AnomalyDetectorBuilder()
                .clusterModel(Mockito.mock(ClusterModel.class))
                .addGoal(Mockito.mock(Goal.class))
                .executor(new ActionExecutorService() {
                    @Override
                    public void start() {

                    }

                    @Override
                    public void shutdown() {

                    }

                    @Override
                    public CompletableFuture<Void> execute(List<Action> actions) {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .build();

        List<Action> actions = List.of(
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 0, 1),
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 1), 0, 2),
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 2), 0, 3),
                new Action(ActionType.MOVE, new TopicPartition("topic-1", 3), 0, 4),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 0), 1, 5),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 1), 2, 6),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 2), 3, 7),
                new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 4, 8),
                new Action(ActionType.MOVE, new TopicPartition("topic-3", 0), 11, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-3", 1), 22, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-3", 2), 33, 10),
                new Action(ActionType.MOVE, new TopicPartition("topic-3", 3), 44, 10));
        List<List<Action>> groupedActions = anomalyDetector.checkAndGroupActions(actions, 2, Map.of(
                "topic-1", 10,
                "topic-2", 5,
                "topic-3", 5
        ));
        Assertions.assertEquals(4, groupedActions.size());
        List<List<Action>> expectedActions = List.of(
                List.of(
                        new Action(ActionType.MOVE, new TopicPartition("topic-1", 0), 0, 1),
                        new Action(ActionType.MOVE, new TopicPartition("topic-1", 1), 0, 2)
                ),
                List.of(
                        new Action(ActionType.MOVE, new TopicPartition("topic-1", 2), 0, 3),
                        new Action(ActionType.MOVE, new TopicPartition("topic-1", 3), 0, 4),
                        new Action(ActionType.MOVE, new TopicPartition("topic-2", 0), 1, 5),
                        new Action(ActionType.MOVE, new TopicPartition("topic-2", 1), 2, 6),
                        new Action(ActionType.MOVE, new TopicPartition("topic-2", 2), 3, 7)
                ),
                List.of(
                        new Action(ActionType.MOVE, new TopicPartition("topic-2", 3), 4, 8),
                        new Action(ActionType.MOVE, new TopicPartition("topic-3", 0), 11, 10),
                        new Action(ActionType.MOVE, new TopicPartition("topic-3", 1), 22, 10)
                ),
                List.of(
                        new Action(ActionType.MOVE, new TopicPartition("topic-3", 2), 33, 10),
                        new Action(ActionType.MOVE, new TopicPartition("topic-3", 3), 44, 10)
                )
        );

        Assertions.assertEquals(expectedActions, groupedActions);
    }

    @Test
    @Timeout(10)
    public void testSchedulingTimeCost() {
        ClusterModel clusterModel = new ClusterModel();

        int brokerNum = 20;
        for (int i = 0; i < brokerNum; i++) {
            clusterModel.registerBroker(i, "");
            clusterModel.updateBrokerMetrics(i, Map.of(
                    RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                    RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                    RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0), System.currentTimeMillis());
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
                Map<Byte, Double> metrics = generateRandomMetrics(r);
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
                .detectIntervalMs(Long.MAX_VALUE)
                .executionIntervalMs(0)
                .executionConcurrency(100)
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
                    public CompletableFuture<Void> execute(List<Action> actions) {
                        actionList.addAll(actions);
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .build();

        TimerUtil timerUtil = new TimerUtil();
        detector.onLeaderChanged(true);
        detector.run();
        Assertions.assertDoesNotThrow(detector::detect0);
        System.out.printf("Detect cost: %d ms, %d actions detected%n", timerUtil.elapsedAs(TimeUnit.MILLISECONDS), actionList.size());
        Assertions.assertFalse(actionList.isEmpty());

        ClusterModelSnapshot snapshot = clusterModel.snapshot();

        double[] loadsBefore = snapshot.brokers().stream().map(b -> b.loadValue(Resource.NW_IN)).mapToDouble(Double::doubleValue).toArray();
        double meanBefore = Arrays.stream(loadsBefore).sum() / loadsBefore.length;
        double stdDevBefore = calculateStdDev(meanBefore, loadsBefore);
        for (Action action : actionList) {
            snapshot.applyAction(action);
        }
        double[] loadsAfter = snapshot.brokers().stream().map(b -> b.loadValue(Resource.NW_IN)).mapToDouble(Double::doubleValue).toArray();
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

    private Map<Byte, Double> generateRandomMetrics(Random r) {
        Map<Byte, Double> metrics = new HashMap<>();
        metrics.put(RawMetricTypes.PARTITION_BYTES_OUT, r.nextDouble(0, 1024 * 1024));
        metrics.put(RawMetricTypes.PARTITION_BYTES_IN, r.nextDouble(0, 1024 * 1024));
        metrics.put(RawMetricTypes.PARTITION_SIZE, 0.0);
        return metrics;
    }

    @Test
    public void testReconfigure() {
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(Collections.emptyMap(), false);
        List<Goal> goals = config.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);

        AnomalyDetector detector = new AnomalyDetectorBuilder()
                .clusterModel(Mockito.mock(ClusterModel.class))
                .detectIntervalMs(Long.MAX_VALUE)
                .addGoals(goals)
                .executor(Mockito.mock(ActionExecutorService.class))
                .build();
        Assertions.assertEquals(2, detector.goals().size());
        Assertions.assertEquals(NetworkInUsageDistributionGoal.class, detector.goals().get(0).getClass());
        Assertions.assertEquals(1024 * 1024, ((AbstractResourceUsageDistributionGoal) detector.goals().get(0)).getUsageDetectThreshold());
        Assertions.assertEquals(0.2, ((AbstractResourceUsageDistributionGoal) detector.goals().get(0)).getUsageAvgDeviationRatio());
        Assertions.assertEquals(NetworkOutUsageDistributionGoal.class, detector.goals().get(1).getClass());
        Assertions.assertEquals(1024 * 1024, ((AbstractResourceUsageDistributionGoal) detector.goals().get(1)).getUsageDetectThreshold());
        Assertions.assertEquals(0.2, ((AbstractResourceUsageDistributionGoal) detector.goals().get(1)).getUsageAvgDeviationRatio());

        detector.reconfigure(Map.of(
                AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, new StringJoiner(",")
                        .add(NetworkInUsageDistributionGoal.class.getName())
                        .add(NetworkOutUsageDistributionGoal.class.getName()).toString(),
                AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 2048 * 1024,
                AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 4096 * 1024,
                AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, 0.3,
                AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, 0.45
        ));

        Assertions.assertEquals(2, detector.goals().size());
        Assertions.assertEquals(NetworkInUsageDistributionGoal.class, detector.goals().get(0).getClass());
        Assertions.assertEquals(2048 * 1024, ((AbstractResourceUsageDistributionGoal) detector.goals().get(0)).getUsageDetectThreshold());
        Assertions.assertEquals(0.3, ((AbstractResourceUsageDistributionGoal) detector.goals().get(0)).getUsageAvgDeviationRatio());
        Assertions.assertEquals(NetworkOutUsageDistributionGoal.class, detector.goals().get(1).getClass());
        Assertions.assertEquals(4096 * 1024, ((AbstractResourceUsageDistributionGoal) detector.goals().get(1)).getUsageDetectThreshold());
        Assertions.assertEquals(0.45, ((AbstractResourceUsageDistributionGoal) detector.goals().get(1)).getUsageAvgDeviationRatio());
    }
}
