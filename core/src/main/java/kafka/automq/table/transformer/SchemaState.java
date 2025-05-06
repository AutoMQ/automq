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

import java.util.Objects;

/**
 * Represents the current schema state
 */
public final class SchemaState {
    private final Schema schema;
    private final boolean isTableSchemaUsed;

    public SchemaState(Schema schema, boolean isTableSchemaUsed) {
        this.schema = schema;
        this.isTableSchemaUsed = isTableSchemaUsed;
    }

    public Schema schema() {
        return schema;
    }

    public boolean isTableSchemaUsed() {
        return isTableSchemaUsed;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (SchemaState) obj;
        return Objects.equals(this.schema, that.schema) &&
            this.isTableSchemaUsed == that.isTableSchemaUsed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, isTableSchemaUsed);
    }

    @Override
    public String toString() {
        return "SchemaState[" +
            "schema=" + schema + ", " +
            "isTableSchemaUsed=" + isTableSchemaUsed + ']';
    }

}
