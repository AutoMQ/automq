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
import kafka.automq.table.process.Transform;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

/**
 * A transform to unwrap a record from a standard {@code ValueRecord} container.
 */
public class ValueUnwrapTransform implements Transform {

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed for this transform.
    }

    @Override
    public GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException {
        if (record == null) {
            return null;
        }
        // Check if the record schema matches the standard ValueRecord structure.
        Object value = record.get(Converter.VALUE_FIELD_NAME);
        Schema.Field field = record.getSchema().getField(Converter.VALUE_FIELD_NAME);

        if (value instanceof GenericRecord && field.schema().getType() == Schema.Type.RECORD) {
            return (GenericRecord) value;
        } else {
            return record;
        }
    }

    @Override
    public String getName() {
        return "ValueUnwrap";
    }
}
