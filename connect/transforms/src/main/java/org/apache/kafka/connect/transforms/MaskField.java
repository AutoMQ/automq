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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MaskField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Mask specified fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>For numeric and string fields, an optional replacement value can be specified that is converted to the correct type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
                    + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    private static final String FIELDS_CONFIG = "fields";
    private static final String REPLACEMENT_CONFIG = "replacement";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "Names of fields to mask.")
            .define(REPLACEMENT_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.LOW, "Custom value replacement, that will be applied to all"
                            + " 'fields' values (numeric or non-empty string values only).");

    private static final String PURPOSE = "mask fields";

    private static final Map<Class<?>, Function<String, ?>> REPLACEMENT_MAPPING_FUNC = new HashMap<>();
    private static final Map<Class<?>, Object> PRIMITIVE_VALUE_MAPPING = new HashMap<>();

    static {
        PRIMITIVE_VALUE_MAPPING.put(Boolean.class, Boolean.FALSE);
        PRIMITIVE_VALUE_MAPPING.put(Byte.class, (byte) 0);
        PRIMITIVE_VALUE_MAPPING.put(Short.class, (short) 0);
        PRIMITIVE_VALUE_MAPPING.put(Integer.class, 0);
        PRIMITIVE_VALUE_MAPPING.put(Long.class, 0L);
        PRIMITIVE_VALUE_MAPPING.put(Float.class, 0f);
        PRIMITIVE_VALUE_MAPPING.put(Double.class, 0d);
        PRIMITIVE_VALUE_MAPPING.put(BigInteger.class, BigInteger.ZERO);
        PRIMITIVE_VALUE_MAPPING.put(BigDecimal.class, BigDecimal.ZERO);
        PRIMITIVE_VALUE_MAPPING.put(Date.class, new Date(0));
        PRIMITIVE_VALUE_MAPPING.put(String.class, "");

        REPLACEMENT_MAPPING_FUNC.put(Byte.class, v -> Values.convertToByte(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Short.class, v -> Values.convertToShort(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Integer.class, v -> Values.convertToInteger(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Long.class, v -> Values.convertToLong(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Float.class, v -> Values.convertToFloat(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Double.class, v -> Values.convertToDouble(null, v));
        REPLACEMENT_MAPPING_FUNC.put(String.class, Function.identity());
        REPLACEMENT_MAPPING_FUNC.put(BigDecimal.class, BigDecimal::new);
        REPLACEMENT_MAPPING_FUNC.put(BigInteger.class, BigInteger::new);
    }

    private Set<String> maskedFields;
    private String replacement;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        maskedFields = new HashSet<>(config.getList(FIELDS_CONFIG));
        replacement = config.getString(REPLACEMENT_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : maskedFields) {
            updatedValue.put(field, masked(value.get(field)));
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            updatedValue.put(field, maskedFields.contains(field.name()) ? masked(origFieldValue) : origFieldValue);
        }
        return newRecord(record, updatedValue);
    }

    private Object masked(Object value) {
        if (value == null) {
            return null;
        }
        return replacement == null ? maskWithNullValue(value) : maskWithCustomReplacement(value, replacement);
    }

    private static Object maskWithCustomReplacement(Object value, String replacement) {
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot mask value of type " + value.getClass() + " with custom replacement.");
        }
        try {
            return replacementMapper.apply(replacement);
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert " + replacement + " (" + replacement.getClass() + ") to number", ex);
        }
    }

    private static Object maskWithNullValue(Object value) {
        Object maskedValue = PRIMITIVE_VALUE_MAPPING.get(value.getClass());
        if (maskedValue == null) {
            if (value instanceof List)
                maskedValue = Collections.emptyList();
            else if (value instanceof Map)
                maskedValue = Collections.emptyMap();
            else
                throw new DataException("Cannot mask value of type: " + value.getClass());
        }
        return maskedValue;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

    public static final class Key<R extends ConnectRecord<R>> extends MaskField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends MaskField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }

}
