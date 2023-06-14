/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.es.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * The timer is not thread-safe, in multiple thread scenario, it may return a not precise value.
 */
public class Timer {
    private final LongAdder count = new LongAdder();
    private final LongAdder timeNanos = new LongAdder();

    public Timer() {
    }

    public void update(long elapseNanos) {
        count.add(1);
        timeNanos.add(elapseNanos);
    }

    /**
     * Get the (count, average time) statistics and reset the timer.
     */
    public Statistics getAndReset() {
        long count = this.count.sumThenReset();
        long timeNanos = this.timeNanos.sumThenReset();
        if (count == 0) {
            return new Statistics(0, 0);
        }
        return new Statistics(count, timeNanos / count);
    }

    static class Statistics {
        public long count;
        public long avg;

        public Statistics(long count, long avg) {
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            String avgStr;
            if (avg > 1000000) {
                avgStr = avg / 1000000 + "ms";
            } else if (avg > 1000) {
                avgStr = avg / 1000 + "us";
            } else {
                avgStr = avg + "ns";
            }
            return "{count=" + count + ", avg=" + avgStr + '}';
        }
    }

}
