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

package kafka.log.es;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SliceRange {
    @JsonProperty("s")
    private long start = Offsets.NOOP_OFFSET;
    @JsonProperty("e")
    private long end = Offsets.NOOP_OFFSET;

    public SliceRange() {}

    public static SliceRange of(long start, long end) {
        SliceRange sliceRange = new SliceRange();
        sliceRange.start(start);
        sliceRange.end(end);
        return sliceRange;
    }

    public long start() {
        return start;
    }

    public void start(long start) {
        this.start = start;
    }

    public long end() {
        return end;
    }

    public void end(long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "[" + start + ", " + end + ']';
    }
}
