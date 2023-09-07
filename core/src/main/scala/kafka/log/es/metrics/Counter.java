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

import java.util.concurrent.atomic.LongAdder;

public class Counter {
    private final LongAdder count = new LongAdder();

    public Counter() {
    }

    public void inc() {
        count.add(1);
    }

    public void inc(int n) {
        count.add(n);
    }

    public Statistics getAndReset() {
        long count = this.count.sumThenReset();
        return new Statistics(count);
    }

    public static class Statistics {
        public long count;

        public Statistics(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "{count=" + count + '}';
        }
    }
}
