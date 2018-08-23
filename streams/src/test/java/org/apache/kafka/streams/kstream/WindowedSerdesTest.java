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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Assert;
import org.junit.Test;

public class WindowedSerdesTest {

    private final String topic = "sample";

    @Test
    public void testTimeWindowSerdeFrom() {
        final Windowed<Integer> timeWindowed = new Windowed<>(10, new TimeWindow(0, Long.MAX_VALUE));
        final Serde<Windowed<Integer>> timeWindowedSerde = WindowedSerdes.timeWindowedSerdeFrom(Integer.class);
        final byte[] bytes = timeWindowedSerde.serializer().serialize(topic, timeWindowed);
        final Windowed<Integer> windowed = timeWindowedSerde.deserializer().deserialize(topic, bytes);
        Assert.assertEquals(timeWindowed, windowed);
    }

    @Test
    public void testSessionWindowedSerdeFrom() {
        final Windowed<Integer> sessionWindowed = new Windowed<>(10, new SessionWindow(0, 1));
        final Serde<Windowed<Integer>> sessionWindowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(Integer.class);
        final byte[] bytes = sessionWindowedSerde.serializer().serialize(topic, sessionWindowed);
        final Windowed<Integer> windowed = sessionWindowedSerde.deserializer().deserialize(topic, bytes);
        Assert.assertEquals(sessionWindowed, windowed);
    }

}
