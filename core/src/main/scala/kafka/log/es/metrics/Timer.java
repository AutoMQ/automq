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

package kafka.log.es.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The timer is not thread-safe, in multiple thread scenario, it may return a not precise value.
 */
public class Timer {
    private final LongAdder count = new LongAdder();
    private final LongAdder timeNanos = new LongAdder();
    private final AtomicLong maxElapseNanos = new AtomicLong();

    public Timer() {
    }

    public void update(long elapseNanos) {
        count.add(1);
        timeNanos.add(elapseNanos);
        for (; ; ) {
            long oldMaxElapseNanos = maxElapseNanos.get();
            if (elapseNanos <= oldMaxElapseNanos) {
                break;
            }
            if (maxElapseNanos.compareAndSet(oldMaxElapseNanos, elapseNanos)) {
                break;
            }
        }
    }

    /**
     * Get the (count, average time) statistics and reset the timer.
     */
    public Statistics getAndReset() {
        long count = this.count.sumThenReset();
        long timeNanos = this.timeNanos.sumThenReset();
        long maxElapseNanos = this.maxElapseNanos.getAndSet(0);
        if (count == 0) {
            return new Statistics(0, 0, maxElapseNanos);
        }
        return new Statistics(count, timeNanos / count, maxElapseNanos);
    }

    public static class Statistics {
        public long count;
        public long avg;
        public long max;

        public Statistics(long count, long avg, long max) {
            this.count = count;
            this.avg = avg;
            this.max = max;
        }

        static String readableNanos(long nanos) {
            String readableStr;
            if (nanos > 1000000) {
                readableStr = nanos / 1000000 + "ms";
            } else if (nanos > 1000) {
                readableStr = nanos / 1000 + "us";
            } else {
                readableStr = nanos + "ns";
            }
            return readableStr;
        }

        @Override
        public String toString() {
            return "{count=" + count + ", avg=" + readableNanos(avg) + ", max=" + readableNanos(max) + '}';
        }
    }

}
