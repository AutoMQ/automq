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

package com.automq.stream.s3.metrics;

import java.util.concurrent.TimeUnit;

public class TimerUtil {
    private long last;

    public TimerUtil() {
        reset();
    }

    public void reset() {
        last = System.nanoTime();
    }

    public long elapsedAs(TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - last, TimeUnit.NANOSECONDS);
    }

    public long elapsedAndResetAs(TimeUnit timeUnit) {
        long now = System.nanoTime();
        long elapsed = timeUnit.convert(now - last, TimeUnit.NANOSECONDS);
        last = now;
        return elapsed;
    }

}
