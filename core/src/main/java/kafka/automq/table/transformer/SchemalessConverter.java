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

package kafka.automq.table.transformer;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.apache.iceberg.types.Types.NestedField.required;

public class SchemalessConverter implements Converter {
    private Schema tableSchema = null;
    private final Types.StructType type = Types.StructType.of(
        required(0, "timestamp", Types.TimestampType.withZone(), "The record timestamp"),
        required(1, "key", Types.StringType.get(), "The record key"),
        required(2, "value", Types.StringType.get(), "The record value")
    );
    private long fieldCount = 0;

    @Override
    public Record convert(org.apache.kafka.common.record.Record src) {
        Schema currentSchema = currentSchema().schema();

        Record record = GenericRecord.create(currentSchema);
        record.setField("timestamp", Instant.ofEpochMilli(src.timestamp()).atOffset(ZoneOffset.UTC));
        fieldCount += 1;
        String keyStr = buf2string(src.key());
        record.setField("key", keyStr);
        fieldCount += FieldMetric.count(keyStr);
        String valueStr = buf2string(src.value());
        record.setField("value", valueStr);
        fieldCount += FieldMetric.count(valueStr);

        return record;
    }

    @Override
    public void tableSchema(Schema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public SchemaState currentSchema() {
        Schema currentSchema = tableSchema != null ? tableSchema : type.asSchema();
        return new SchemaState(currentSchema, tableSchema != null);
    }

    @Override
    public long fieldCount() {
        return fieldCount;
    }

    private static String buf2string(ByteBuffer buf) {
        if (buf == null) {
            return "";
        }
        byte[] bytes = new byte[buf.remaining()];
        buf.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
