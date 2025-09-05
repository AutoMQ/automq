/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.table.process;

import kafka.automq.table.process.exception.TransformException;
import kafka.automq.table.process.transform.SchemalessTransform;

import org.apache.kafka.common.record.Record;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class SchemalessTransformTest {

    @Mock
    private Record kafkaRecord;

    @Mock
    private GenericRecord inputRecord;

    @Mock
    private TransformContext context;

    private SchemalessTransform transform;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        transform = new SchemalessTransform();
        when(context.getKafkaRecord()).thenReturn(kafkaRecord);
    }

    @Test
    public void testApply_WithKeyAndValue() throws TransformException {
        // Arrange
        long timestamp = 1234567890L;

        when(kafkaRecord.hasKey()).thenReturn(true);
        when(kafkaRecord.key()).thenReturn(ByteBuffer.wrap("321".getBytes()));
        when(kafkaRecord.hasValue()).thenReturn(true);
        when(kafkaRecord.value()).thenReturn(ByteBuffer.wrap("123".getBytes()));
        when(kafkaRecord.timestamp()).thenReturn(timestamp);

        // Act
        GenericRecord result = transform.apply(inputRecord, context);

        // Assert
        assertNotNull(result);
        assertEquals(SchemalessTransform.SCHEMALESS_SCHEMA, result.getSchema());
        assertEquals("321", (String) result.get("key"));
        assertEquals("123",  (String) result.get("value"));
        assertEquals(timestamp, result.get("timestamp"));
    }

    @Test
    public void testApply_WithoutKeyAndValue() throws TransformException {
        // Arrange
        long timestamp = 1234567890L;

        when(kafkaRecord.hasKey()).thenReturn(false);
        when(kafkaRecord.hasValue()).thenReturn(false);
        when(kafkaRecord.timestamp()).thenReturn(timestamp);

        // Act
        GenericRecord result = transform.apply(inputRecord, context);

        // Assert
        assertNotNull(result);
        assertEquals(SchemalessTransform.SCHEMALESS_SCHEMA, result.getSchema());
        assertNull(result.get("key"));
        assertNull(result.get("value"));
        assertEquals(timestamp, result.get("timestamp"));
    }

    @Test
    public void testGetName() {
        // Act & Assert
        assertEquals("schemaless", transform.getName());
    }
}
