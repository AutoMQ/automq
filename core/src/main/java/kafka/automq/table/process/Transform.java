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

import org.apache.avro.generic.GenericRecord;

import java.util.Map;

/**
 * Transform interface for Avro GenericRecord data processing.
 *
 * <p>Stateless, chainable transformations for content manipulation like CDC unwrapping,
 * field mapping, and data enrichment. Implementations must be thread-safe.</p>
 *
 * @see Converter
 * @see TransformException
 */
public interface Transform {

    /**
     * Configures the transform with operation-specific settings.
     *
     * @param configs configuration parameters, must not be null
     * @throws IllegalArgumentException if configs is null or contains invalid values
     */
    void configure(Map<String, ?> configs);

    /**
     * Applies transformation to the input GenericRecord.
     *
     * @param record the input GenericRecord, must not be null
     * @param context contextual information for the transformation
     * @return the transformed GenericRecord, must not be null
     * @throws TransformException if transformation fails
     * @throws IllegalArgumentException if record or context is null
     */
    GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException;

    /**
     * Returns a descriptive name for this transform.
     *
     * @return transform name, never null
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }
}
