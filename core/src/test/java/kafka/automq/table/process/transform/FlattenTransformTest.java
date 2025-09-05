
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

package kafka.automq.table.process.transform;

import kafka.automq.table.process.RecordAssembler;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Tag("S3Unit")
class FlattenTransformTest {

    private static final Schema INNER_SCHEMA =
        SchemaBuilder.record("Inner")
            .fields()
            .requiredLong("id")
            .requiredString("data")
            .endRecord();

    private static final Schema VALUE_CONTAINER_SCHEMA =
        SchemaBuilder.record("ValueContainer")
            .fields()
            .name(RecordAssembler.KAFKA_VALUE_FIELD).type(INNER_SCHEMA).noDefault()
            .endRecord();

    private static final Schema NON_WRAPPED_SCHEMA =
        SchemaBuilder.record("NonWrapped")
            .fields()
            .requiredLong("id")
            .endRecord();

    private static final Schema STRING_VALUE_SCHEMA =
        SchemaBuilder.record("StringValue")
            .fields()
            .requiredString(RecordAssembler.KAFKA_VALUE_FIELD)
            .endRecord();


    private FlattenTransform transform;
    private TransformContext context;

    @BeforeEach
    void setUp() {
        transform = new FlattenTransform();
        transform.configure(Collections.emptyMap());
        context = mock(TransformContext.class);
    }

    @Test
    void testApplyWhenRecordIsWrappedShouldUnwrap() throws TransformException {
        GenericRecord innerRecord = new GenericRecordBuilder(INNER_SCHEMA)
            .set("id", 1L)
            .set("data", "test")
            .build();

        GenericRecord outerRecord = new GenericRecordBuilder(VALUE_CONTAINER_SCHEMA)
            .set(RecordAssembler.KAFKA_VALUE_FIELD, innerRecord)
            .build();

        GenericRecord result = transform.apply(outerRecord, context);

        assertSame(innerRecord, result, "The transform should return the inner record.");
    }

    @Test
    void testApplyWhenRecordIsNotWrappedShouldThrowException() {
        GenericRecord nonWrappedRecord = new GenericRecordBuilder(NON_WRAPPED_SCHEMA)
            .set("id", 123L)
            .build();

        TransformException e = assertThrows(TransformException.class, () -> transform.apply(nonWrappedRecord, context));
        assertTrue(e.getMessage().contains("Record is null or has no value field"));
    }

}
