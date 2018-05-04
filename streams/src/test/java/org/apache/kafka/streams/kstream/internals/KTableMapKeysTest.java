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
package org.apache.kafka.streams.kstream.internals;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KTableMapKeysTest {

    final private Serde<String> stringSerde = new Serdes.StringSerde();
    final private Serde<Integer>  integerSerde = new Serdes.IntegerSerde();
    private File stateDir = null;
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    
    @Before
     public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testMapKeysConvertingToStream() {
        final StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic_map_keys";

        KTable<Integer, String> table1 = builder.table(topic1, Consumed.with(integerSerde, stringSerde));

        final Map<Integer, String> keyMap = new HashMap<>();
        keyMap.put(1, "ONE");
        keyMap.put(2, "TWO");
        keyMap.put(3, "THREE");

        KeyValueMapper<Integer, String, String> keyMapper = new KeyValueMapper<Integer, String, String>() {
            @Override
            public  String apply(Integer key, String value) {
                return keyMap.get(key);
            }
        };

        KStream<String, String> convertedStream = table1.toStream(keyMapper);

        final String[] expected = new String[]{"ONE:V_ONE", "TWO:V_TWO", "THREE:V_THREE"};
        final int[] originalKeys = new int[]{1, 2, 3};
        final String[] values = new String[]{"V_ONE", "V_TWO", "V_THREE"};

        MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();

        convertedStream.process(supplier);

        driver.setUp(builder, stateDir);
        for (int i = 0;  i < originalKeys.length; i++) {
            driver.process(topic1, originalKeys[i], values[i]);
        }
        driver.flushState();

        assertEquals(3, supplier.theCapturedProcessor().processed.size());

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], supplier.theCapturedProcessor().processed.get(i));
        }
    }
}