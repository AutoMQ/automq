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
import org.apache.iceberg.data.Record;

public interface Converter {
    /**
     * Convert a Kafka record to an Iceberg record
     * * 1. If the table is not configured, create an Iceberg record based on the record's schema
     * * 2. If the table is configured, create an Iceberg record based on the table's schema
     * * 3. If the record's schema is inconsistent with the table's schema, create an Iceberg record based on the record's schema
     * @param record Kafka record
     * @return Iceberg record
     */
    Record convert(org.apache.kafka.common.record.Record record);

    /**
     * Configure the table for the converter
     * when converting, the fieldId should be consistent with the actual fieldId
     * if the table is not configured, create an Iceberg record based on the record's schema,
     * the fieldId in the record may not be consistent with the actual fieldId
     * @param tableSchema the Iceberg table schema
     */
    void tableSchema(Schema tableSchema);

    /**
     * Get current schema information
     * @return Current schema state including ID and schema
     */
    SchemaState currentSchema();

    /**
     * Get processed field count.
     * @return processed field count
     */
    long fieldCount();
}
