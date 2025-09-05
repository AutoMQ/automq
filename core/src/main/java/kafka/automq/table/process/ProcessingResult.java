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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Objects;

/**
 * Comprehensive result object returned by RecordProcessor operations.
 *
 * <ul>
 *   <li><strong>Success</strong>: Contains finalRecord, finalSchema, and
 *       finalSchemaIdentity; error is null</li>
 *   <li><strong>Failure</strong>: Contains detailed error information;
 *       all other fields are null</li>
 * </ul>
 *
 * @see RecordProcessor#process(int, org.apache.kafka.common.record.Record)
 * @see DataError
 */
public final class ProcessingResult {

    private final GenericRecord finalRecord;
    private final Schema finalSchema;
    private final String finalSchemaIdentity;
    private final DataError error;

    /**
     * Creates a successful processing result.
     *
     * @param finalRecord the Avro GenericRecord ready for further processing, must not be null
     * @param finalSchema the Avro schema matching the finalRecord, must not be null
     * @param finalSchemaIdentity unique identifier for schema comparison, must not be null
     * @throws IllegalArgumentException if any parameter is null
     */
    public ProcessingResult(GenericRecord finalRecord, Schema finalSchema, String finalSchemaIdentity) {
        this.finalRecord = Objects.requireNonNull(finalRecord, "finalRecord cannot be null");
        this.finalSchema = Objects.requireNonNull(finalSchema, "finalSchema cannot be null");
        this.finalSchemaIdentity = Objects.requireNonNull(finalSchemaIdentity, "finalSchemaIdentity cannot be null");
        this.error = null;
    }

    /**
     * Creates a failed processing result.
     *
     * @param error the data error that occurred during processing, must not be null
     * @throws IllegalArgumentException if error is null
     */
    public ProcessingResult(DataError error) {
        this.finalRecord = null;
        this.finalSchema = null;
        this.finalSchemaIdentity = null;
        this.error = Objects.requireNonNull(error, "error cannot be null");
    }

    public GenericRecord getFinalRecord() {
        return finalRecord;
    }
    public Schema getFinalSchema() {
        return finalSchema;
    }
    public String getFinalSchemaIdentity() {
        return finalSchemaIdentity;
    }
    public DataError getError() {
        return error;
    }
    public boolean isSuccess() {
        return error == null;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        ProcessingResult that = (ProcessingResult) obj;
        return Objects.equals(finalRecord, that.finalRecord) &&
               Objects.equals(finalSchema, that.finalSchema) &&
               Objects.equals(finalSchemaIdentity, that.finalSchemaIdentity) &&
               Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finalRecord, finalSchema, finalSchemaIdentity, error);
    }

    @Override
    public String toString() {
        if (!isSuccess()) {
            return "ProcessingResult{success=true, schemaIdentity=" + finalSchemaIdentity + "}";
        } else {
            return "ProcessingResult{success=false, error=" + error + "}";
        }
    }
}
