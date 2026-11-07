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
package kafka.automq.table.binder;

import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;

/**
 * Represents the mapping between an Avro field and its corresponding Iceberg field.
 * This class stores the position, key, schema, and type information needed to
 * convert field values during record binding.
 */
public class FieldMapping {
    private final int avroPosition;
    private final String avroKey;
    private final Type icebergType;
    private final Schema avroSchema;

    public FieldMapping(int avroPosition, String avroKey, Type icebergType, Schema avroSchema) {
        this.avroPosition = avroPosition;
        this.avroKey = avroKey;
        this.icebergType = icebergType;
        this.avroSchema = avroSchema;
    }

    public int avroPosition() {
        return avroPosition;
    }

    public String avroKey() {
        return avroKey;
    }

    public Type icebergType() {
        return icebergType;
    }

    public Schema avroSchema() {
        return avroSchema;
    }
}
