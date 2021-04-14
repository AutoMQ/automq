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
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LogAndSkipOnInvalidTimestampTest extends TimestampExtractorTest {

    @Test
    public void extractMetadataTimestamp() {
        testExtractMetadataTimestamp(new LogAndSkipOnInvalidTimestamp());
    }

    @Test
    public void logAndSkipOnInvalidTimestamp() {
        final long invalidMetadataTimestamp = -42;

        final TimestampExtractor extractor = new LogAndSkipOnInvalidTimestamp();
        final long timestamp = extractor.extract(
            new ConsumerRecord<>(
                "anyTopic",
                0,
                0,
                invalidMetadataTimestamp,
                TimestampType.NO_TIMESTAMP_TYPE,
                0,
                0,
                null,
                null,
                new RecordHeaders(),
                Optional.empty()),
            0
        );

        assertThat(timestamp, is(invalidMetadataTimestamp));
    }

}
