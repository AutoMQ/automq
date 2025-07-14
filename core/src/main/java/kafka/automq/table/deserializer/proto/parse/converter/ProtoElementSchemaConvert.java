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

package kafka.automq.table.deserializer.proto.parse.converter;

import kafka.automq.table.deserializer.proto.schema.EnumDefinition;
import kafka.automq.table.deserializer.proto.schema.MessageDefinition;

import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import kotlin.ranges.IntRange;

public class ProtoElementSchemaConvert implements ProtoElementConvert {

    @Override
    public EnumDefinition convert(EnumElement enumElement) {
        Boolean allowAlias = getOptionBoolean(ProtoConstants.ALLOW_ALIAS_OPTION, enumElement.getOptions());
        EnumDefinition.Builder builder = EnumDefinition.newBuilder(enumElement.getName(), allowAlias);

        for (EnumConstantElement constant : enumElement.getConstants()) {
            builder.addValue(constant.getName(), constant.getTag());
        }

        return builder.build();
    }

    @Override
    public MessageDefinition convert(MessageElement messageElement) {
        MessageDefinition.Builder message = MessageDefinition.newBuilder(messageElement.getName());

        // Process nested types
        for (TypeElement type : messageElement.getNestedTypes()) {
            if (type instanceof MessageElement) {
                message.addMessageDefinition(convert((MessageElement) type));
            } else if (type instanceof EnumElement) {
                message.addEnumDefinition(convert((EnumElement) type));
            }
        }

        // Process fields
        processMessageFields(message, messageElement);

        // Process reserved ranges and names
        processReservedElements(message, messageElement);

        // Process options
        processMessageOptions(message, messageElement);

        return message.build();
    }

    private void processMessageFields(MessageDefinition.Builder message, MessageElement messageElement) {
        Set<String> processedFields = new HashSet<>();

        // Process oneofs first
        for (OneOfElement oneof : messageElement.getOneOfs()) {
            MessageDefinition.OneofBuilder oneofBuilder = message.addOneof(oneof.getName());
            for (FieldElement field : oneof.getFields()) {
                processedFields.add(field.getName());
                addFieldToOneof(oneofBuilder, field);
            }
        }

        // Process regular fields
        for (FieldElement field : messageElement.getFields()) {
            if (!processedFields.contains(field.getName())) {
                addFieldToMessage(message, field);
            }
        }
    }

    private void addFieldToMessage(MessageDefinition.Builder message, FieldElement field) {
        Field.Label fieldLabel = field.getLabel();
        String label = fieldLabel != null ? fieldLabel.toString().toLowerCase(Locale.ENGLISH) : null;
        String fieldType = field.getType();
        ProtoType protoType = ProtoType.get(fieldType);

        // Handle map fields
        if (protoType.isMap()) {
            ProtoType keyType = protoType.getKeyType();
            ProtoType valueType = protoType.getValueType();
            if (keyType != null && valueType != null) {
                processMapField(message, field, keyType, valueType);
                return;
            }
        }

        message.addField(label, fieldType, field.getName(), field.getTag(),
            field.getDefaultValue(), field.getJsonName(),
            getOptionBoolean(ProtoConstants.PACKED_OPTION, field.getOptions()));
    }

    private void processMapField(MessageDefinition.Builder message, FieldElement field,
        ProtoType keyType, ProtoType valueType) {
        String mapEntryName = toMapEntryName(field.getName());
        MessageDefinition.Builder mapMessage = MessageDefinition.newBuilder(mapEntryName);
        mapMessage.setMapEntry(true);

        mapMessage.addField(null, keyType.getSimpleName(), ProtoConstants.KEY_FIELD, 1, null, null, null);
        mapMessage.addField(null, valueType.getSimpleName(), ProtoConstants.VALUE_FIELD, 2, null, null, null);

        message.addMessageDefinition(mapMessage.build());
        message.addField("repeated", mapEntryName, field.getName(), field.getTag(),
            null, field.getJsonName(), null);
    }

    private void addFieldToOneof(MessageDefinition.OneofBuilder oneof, FieldElement field) {
        oneof.addField(field.getType(), field.getName(), field.getTag(),
            field.getDefaultValue(), field.getJsonName());
    }

    private void processReservedElements(MessageDefinition.Builder message, MessageElement messageElement) {
        for (ReservedElement reserved : messageElement.getReserveds()) {
            for (Object value : reserved.getValues()) {
                if (value instanceof String) {
                    message.addReservedName((String) value);
                } else if (value instanceof Integer) {
                    int tag = (Integer) value;
                    message.addReservedRange(tag, tag);
                } else if (value instanceof IntRange) {
                    IntRange range = (IntRange) value;
                    message.addReservedRange(range.getStart(), range.getEndInclusive());
                }
            }
        }
    }

    private void processMessageOptions(MessageDefinition.Builder message, MessageElement messageElement) {
        Boolean isMapEntry = getOptionBoolean(ProtoConstants.MAP_ENTRY_OPTION, messageElement.getOptions());
        if (isMapEntry != null) {
            message.setMapEntry(isMapEntry);
        }
    }

    private static Boolean getOptionBoolean(String name, List<OptionElement> options) {
        return findOption(name, options)
            .map(o -> Boolean.valueOf(o.getValue().toString()))
            .orElse(null);
    }

    private static Optional<OptionElement> findOption(String name, List<OptionElement> options) {
        return options.stream()
            .filter(o -> o.getName().equals(name))
            .findFirst();
    }

    private static String toMapEntryName(String fieldName) {
        return Character.toUpperCase(fieldName.charAt(0)) +
            fieldName.substring(1) +
            ProtoConstants.MAP_ENTRY_SUFFIX;
    }
}
