/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.es.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The timer is not thread-safe, in multiple thread scenario, it may return a not precise value.
 */
public class Timer {
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong timeNanos = new AtomicLong();

    public Timer() {}

    public void update(long elapseNanos) {
       count.incrementAndGet();
       timeNanos.addAndGet(elapseNanos);
    }

    public long getAndReset() {
        long count = this.count.getAndSet(0);
        long timeNanos = this.timeNanos.getAndSet(0);
        if (count == 0) {
            return 0;
        }
        return timeNanos / count;
    }

}
