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

package org.apache.kafka.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class WeightedRandomList<T> {
    private final List<Entity<T>> entityList = new ArrayList<>();
    private final List<Double> cumulativeNormalizedWeights = new ArrayList<>();
    private final Random r;

    public WeightedRandomList() {
        this(new Random());
    }

    public WeightedRandomList(Random r) {
        this.r = r;
    }

    public void add(Entity<T> entity) {
        entityList.add(entity);
    }

    public void update() {
        double totalWeight = 0.0;
        List<Double> cumulativeWeights = new ArrayList<>();
        for (Entity<T> entity : entityList) {
            totalWeight += entity.weight();
            cumulativeWeights.add(totalWeight);
        }
        cumulativeNormalizedWeights.clear();
        for (double cumulativeWeight : cumulativeWeights) {
            cumulativeNormalizedWeights.add(cumulativeWeight / totalWeight);
        }
    }

    public Entity<T> next() {
        return entityList.get(nextIndex(this.r.nextDouble()));
    }

    private int nextIndex(double r) {
        if (r < 0.0 || r > 1.0) {
            throw new IllegalArgumentException("r must be in [0.0, 1.0]");
        }
        int result = Collections.binarySearch(cumulativeNormalizedWeights, r);
        return Math.max(0, Math.min(entityList.size() - 1, result < 0 ? -result - 1 : result));
    }

    public int size() {
        return entityList.size();
    }

    public static class Entity<T> {
        private final T value;
        private double weight;

        public Entity(T value, double weight) {
            this.value = value;
            this.weight = weight;
        }

        public T entity() {
            return value;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        public double weight() {
            return weight;
        }
    }
}
