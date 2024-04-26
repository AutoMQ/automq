/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.goals;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.model.ClusterModelSnapshot;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GoalUtils {

    public static boolean isValidAction(Action action, ClusterModelSnapshot cluster) {
        if (cluster.broker(action.getSrcBrokerId()) == null
                || cluster.broker(action.getDestBrokerId()) == null
                || cluster.replica(action.getSrcBrokerId(), action.getSrcTopicPartition()) == null) {
            return false;
        }
        if (action.getType() == ActionType.SWAP) {
            return action.getDestTopicPartition() != null
                    && cluster.replica(action.getDestBrokerId(), action.getDestTopicPartition()) != null;
        }
        return true;
    }

    public static double linearNormalization(double value, double max, double min) {
        return linearNormalization(value, max, min, false);
    }

    public static double linearNormalization(double value, double max, double min, boolean reverse) {
        if (reverse) {
            return 1 - ((value - min) / (max - min));
        }
        return (value - min) / (max - min);
    }

    public static Map<String, Set<String>> groupGoals(Collection<Goal> goals) {
        return goals.stream().collect(Collectors.groupingBy(Goal::group, Collectors.mapping(Goal::name, Collectors.toSet())));
    }
}
