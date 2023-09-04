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

package kafka.autobalancer.goals;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.model.ClusterModelSnapshot;

import java.util.HashMap;
import java.util.Map;

public class GoalUtils {
    private static final Map<String, Integer> GOALS_PRIORITY_MAP = new HashMap<>();

    static {
        GOALS_PRIORITY_MAP.put(NetworkInCapacityGoal.class.getSimpleName(), 10);
        GOALS_PRIORITY_MAP.put(NetworkOutCapacityGoal.class.getSimpleName(), 10);
        GOALS_PRIORITY_MAP.put(NetworkInDistributionGoal.class.getSimpleName(), 8);
        GOALS_PRIORITY_MAP.put(NetworkOutDistributionGoal.class.getSimpleName(), 8);
    }

    public static int priority(AbstractGoal goal) {
        return GOALS_PRIORITY_MAP.getOrDefault(goal.name(), 0);
    }

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

    public static double normalize(double value, double max, double min) {
        return normalize(value, max, min, false);
    }

    public static double normalize(double value, double max, double min, boolean reverse) {
        if (reverse) {
            return 1 - (value - min) / (max - min);
        }
        return (value - min) / (max - min);
    }
}
