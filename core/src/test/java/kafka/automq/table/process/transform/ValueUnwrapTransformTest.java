
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

import kafka.automq.table.process.Converter;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ValueUnwrapTransformTest {

    private static final Schema INNER_SCHEMA =
        SchemaBuilder.record("Inner")
            .fields()
            .requiredLong("id")
            .requiredString("data")
            .endRecord();

    private static final Schema VALUE_CONTAINER_SCHEMA =
        SchemaBuilder.record("ValueContainer")
            .fields()
            .name(Converter.VALUE_FIELD_NAME).type(INNER_SCHEMA).noDefault()
            .endRecord();

    private static final Schema NON_WRAPPED_SCHEMA =
        SchemaBuilder.record("NonWrapped")
            .fields()
            .requiredLong("id")
            .endRecord();

    private static final Schema STRING_VALUE_SCHEMA =
        SchemaBuilder.record("StringValue")
            .fields()
            .requiredString(Converter.VALUE_FIELD_NAME)
            .endRecord();


    private ValueUnwrapTransform transform;
    private TransformContext context;

    @BeforeEach
    void setUp() {
        transform = new ValueUnwrapTransform();
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
            .set(Converter.VALUE_FIELD_NAME, innerRecord)
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
        assertTrue(e.getMessage().contains("does not contain a 'value' field to unwrap"));
    }

    @Test
    void testApplyWhenValueIsNotRecordShouldReturnOriginal() throws TransformException {
        GenericRecord stringValueRecord = new GenericRecordBuilder(STRING_VALUE_SCHEMA)
            .set(Converter.VALUE_FIELD_NAME, "just a string")
            .build();

        GenericRecord result = transform.apply(stringValueRecord, context);

        assertSame(stringValueRecord, result, "The transform should return the original record if the 'value' field is not a record.");
    }

    @Test
    void testApplyWhenRecordIsNullShouldReturnNull() throws TransformException {
        GenericRecord result = transform.apply(null, context);
        assertNull(result, "The transform should return null when the input record is null.");
    }

}
