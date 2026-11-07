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

package kafka.autobalancer.metricsreporter.metric;

// NotThreadSafe: The caller should ensure thread safety.
public class Derivator {
    private long lastY = 0;
    private long lastX = 0;

    public double derive(long x, long y) {
        return derive(x, y, true);
    }

    public double derive(long x, long y, boolean ignoreZero) {
        if (lastX == 0 && ignoreZero) {
            lastX = x;
            lastY = y;
            return 0.0;
        }
        double deltaX = x - lastX;
        double result;
        if (deltaX == 0) {
            result = 0.0;
        } else {
            result = (y - lastY) / deltaX;
        }
        lastX = x;
        lastY = y;
        return result;
    }

    public void reset() {
        lastX = 0;
        lastY = 0;
    }
}
