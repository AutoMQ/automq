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
package org.apache.kafka.streams.state.internals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import static org.apache.kafka.streams.state.internals.OffsetCheckpoint.writeEntry;
import static org.apache.kafka.streams.state.internals.OffsetCheckpoint.writeIntLine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class OffsetCheckpointTest {

    private final String topic = "topic";

    @Test
    public void testReadWrite() throws IOException {
        final File f = TestUtils.tempFile();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

        try {
            final Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), 0L);
            offsets.put(new TopicPartition(topic, 1), 1L);
            offsets.put(new TopicPartition(topic, 2), 2L);

            checkpoint.write(offsets);
            assertEquals(offsets, checkpoint.read());

            checkpoint.delete();
            assertFalse(f.exists());

            offsets.put(new TopicPartition(topic, 3), 3L);
            checkpoint.write(offsets);
            assertEquals(offsets, checkpoint.read());
        } finally {
            checkpoint.delete();
        }
    }

    @Test
    public void shouldNotWriteCheckpointWhenNoOffsets() throws IOException {
        // we do not need to worry about file name uniqueness since this file should not be created
        final File f = new File(TestUtils.tempDirectory().getAbsolutePath(), "kafka.tmp");
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

        checkpoint.write(Collections.<TopicPartition, Long>emptyMap());

        assertFalse(f.exists());

        assertEquals(Collections.<TopicPartition, Long>emptyMap(), checkpoint.read());

        // deleting a non-exist checkpoint file should be fine
        checkpoint.delete();
    }

    @Test
    public void shouldSkipNegativeOffsetsDuringRead() throws IOException {
        final File file = TestUtils.tempFile();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(file);

        try {
            final Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), -1L);

            writeVersion0(offsets, file);
        } finally {
            checkpoint.delete();
        }
    }

    @Test
    public void shouldThrowOnNegativeOffsetInWrite() throws IOException {
        final File f = TestUtils.tempFile();
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

        try {
            final Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), 0L);
            offsets.put(new TopicPartition(topic, 1), -1L);
            offsets.put(new TopicPartition(topic, 2), 2L);

            assertThrows(IllegalStateException.class, () -> checkpoint.write(offsets));
        } finally {
            checkpoint.delete();
        }
    }

    /**
     * Write all the offsets following the version 0 format without any verification (eg enforcing offsets >= 0)
     */
    static void writeVersion0(final Map<TopicPartition, Long> offsets, final File file) throws IOException {
        final FileOutputStream fileOutputStream = new FileOutputStream(file);
        try (final BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
            writeIntLine(writer, 0);
            writeIntLine(writer, offsets.size());

            for (final Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                final TopicPartition tp = entry.getKey();
                final Long offset = entry.getValue();
                writeEntry(writer, tp, offset);
            }

            writer.flush();
            fileOutputStream.getFD().sync();
        }
    }
}
