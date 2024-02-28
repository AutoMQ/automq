/*
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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

/**
 * <p>
 *     A timestamp representing an absolute time, without timezone information. The corresponding Java type is a
 *     {@link java.util.Date}. The underlying representation is a long representing the number of milliseconds since Unix epoch.
 * </p>
 */
public class Timestamp {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Timestamp";

    /**
     * Returns a SchemaBuilder for a Timestamp. By returning a SchemaBuilder you can override additional schema settings such
     * as required/optional, default value, and documentation.
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final Schema SCHEMA = builder().schema();

    /**
     * Convert a value from its logical format ({@link java.util.Date}) to its encoded format (long).
     * @param value the logical value
     * @return the encoded value
     */
    public static long fromLogical(Schema schema, java.util.Date value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of Timestamp object but the schema does not match.");
        return value.getTime();
    }

    /**
     * Convert a value from its encoded format (long) to its logical format ({@link java.util.Date}).
     * @param value the encoded value
     * @return the logical value
     */
    public static java.util.Date toLogical(Schema schema, long value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of Timestamp object but the schema does not match.");
        return new java.util.Date(value);
    }
}
