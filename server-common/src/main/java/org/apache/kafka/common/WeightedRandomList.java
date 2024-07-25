/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
