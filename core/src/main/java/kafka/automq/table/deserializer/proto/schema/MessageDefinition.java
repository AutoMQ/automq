/*
 * Copyright 2021 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.automq.table.deserializer.proto.schema;

import com.google.protobuf.DescriptorProtos;

import java.util.HashMap;
import java.util.Map;

public class MessageDefinition {
    // --- public static ---

    public static Builder newBuilder(String msgTypeName) {
        return new Builder(msgTypeName);
    }

    // --- public ---

    public String toString() {
        return mMsgType.toString();
    }

    // --- package ---

    DescriptorProtos.DescriptorProto getMessageType() {
        return mMsgType;
    }

    // --- private ---

    private MessageDefinition(DescriptorProtos.DescriptorProto msgType) {
        mMsgType = msgType;
    }

    private DescriptorProtos.DescriptorProto mMsgType;

    /**
     * MessageDefinition.Builder
     */
    public static class Builder {
        // --- public ---

        public Builder addField(String label, String type, String name, int num, String defaultVal) {
            return addField(label, type, name, num, defaultVal, null, null);
        }

        public Builder addField(String label, String type, String name, int num, String defaultVal,
                String jsonName, Boolean isPacked) {
            DescriptorProtos.FieldDescriptorProto.Label protoLabel = sLabelMap.get(label);
            doAddField(protoLabel, type, name, num, defaultVal, jsonName, isPacked, null);
            return this;
        }

        public OneofBuilder addOneof(String oneofName) {
            mMsgTypeBuilder.addOneofDecl(
                    DescriptorProtos.OneofDescriptorProto.newBuilder().setName(oneofName).build());
            return new OneofBuilder(this, mOneofIndex++);
        }

        public Builder addMessageDefinition(MessageDefinition msgDef) {
            mMsgTypeBuilder.addNestedType(msgDef.getMessageType());
            return this;
        }

        public Builder addEnumDefinition(EnumDefinition enumDef) {
            mMsgTypeBuilder.addEnumType(enumDef.getEnumType());
            return this;
        }

        // Note: added
        public Builder addReservedName(String reservedName) {
            mMsgTypeBuilder.addReservedName(reservedName);
            return this;
        }

        // Note: added
        public Builder addReservedRange(int start, int end) {
            DescriptorProtos.DescriptorProto.ReservedRange.Builder rangeBuilder = DescriptorProtos.DescriptorProto.ReservedRange
                    .newBuilder();
            rangeBuilder.setStart(start).setEnd(end);
            mMsgTypeBuilder.addReservedRange(rangeBuilder.build());
            return this;
        }

        // Note: added
        public Builder setMapEntry(boolean mapEntry) {
            DescriptorProtos.MessageOptions.Builder optionsBuilder = DescriptorProtos.MessageOptions
                    .newBuilder();
            optionsBuilder.setMapEntry(mapEntry);
            mMsgTypeBuilder.mergeOptions(optionsBuilder.build());
            return this;
        }

        public MessageDefinition build() {
            return new MessageDefinition(mMsgTypeBuilder.build());
        }

        // --- private ---

        private Builder(String msgTypeName) {
            mMsgTypeBuilder = DescriptorProtos.DescriptorProto.newBuilder();
            mMsgTypeBuilder.setName(msgTypeName);
        }

        private void doAddField(DescriptorProtos.FieldDescriptorProto.Label label, String type, String name,
                int num, String defaultVal, String jsonName, Boolean isPacked, OneofBuilder oneofBuilder) {
            DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder = DescriptorProtos.FieldDescriptorProto
                    .newBuilder();
            // Note: changed
            if (label != null) {
                fieldBuilder.setLabel(label);
            }
            DescriptorProtos.FieldDescriptorProto.Type primType = sTypeMap.get(type);
            if (primType != null) {
                fieldBuilder.setType(primType);
            } else {
                fieldBuilder.setTypeName(type);
            }
            fieldBuilder.setName(name).setNumber(num);
            if (defaultVal != null) {
                fieldBuilder.setDefaultValue(defaultVal);
            }
            if (oneofBuilder != null) {
                fieldBuilder.setOneofIndex(oneofBuilder.getIdx());
            }
            if (jsonName != null) {
                fieldBuilder.setJsonName(jsonName);
            }
            if (isPacked != null) {
                DescriptorProtos.FieldOptions.Builder optionsBuilder = DescriptorProtos.FieldOptions
                        .newBuilder();
                optionsBuilder.setPacked(isPacked);
                fieldBuilder.mergeOptions(optionsBuilder.build());
            }
            mMsgTypeBuilder.addField(fieldBuilder.build());
        }

        private DescriptorProtos.DescriptorProto.Builder mMsgTypeBuilder;
        private int mOneofIndex = 0;
    }

    /**
     * MessageDefinition.OneofBuilder
     */
    public static class OneofBuilder {
        // --- public ---

        public OneofBuilder addField(String type, String name, int num, String defaultVal) {
            return addField(type, name, num, defaultVal, null);
        }

        public OneofBuilder addField(String type, String name, int num, String defaultVal, String jsonName) {
            mMsgBuilder.doAddField(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, type, name,
                    num, defaultVal, jsonName, null, this);
            return this;
        }

        public Builder msgDefBuilder() {
            return mMsgBuilder;
        }

        public int getIdx() {
            return mIdx;
        }

        // --- private ---

        private OneofBuilder(Builder msgBuilder, int oneofIdx) {
            mMsgBuilder = msgBuilder;
            mIdx = oneofIdx;
        }

        private Builder mMsgBuilder;
        private int mIdx;
    }

    // --- private static ---

    private static Map<String, DescriptorProtos.FieldDescriptorProto.Type> sTypeMap;
    private static Map<String, DescriptorProtos.FieldDescriptorProto.Label> sLabelMap;

    static {
        sTypeMap = new HashMap<String, DescriptorProtos.FieldDescriptorProto.Type>();
        sTypeMap.put("double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE);
        sTypeMap.put("float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT);
        sTypeMap.put("int32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        sTypeMap.put("int64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
        sTypeMap.put("uint32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32);
        sTypeMap.put("uint64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64);
        sTypeMap.put("sint32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32);
        sTypeMap.put("sint64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64);
        sTypeMap.put("fixed32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
        sTypeMap.put("fixed64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64);
        sTypeMap.put("sfixed32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32);
        sTypeMap.put("sfixed64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64);
        sTypeMap.put("bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
        sTypeMap.put("string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        sTypeMap.put("bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);
        // sTypeMap.put("enum", FieldDescriptorProto.Type.TYPE_ENUM);
        // sTypeMap.put("message", FieldDescriptorProto.Type.TYPE_MESSAGE);
        // sTypeMap.put("group", FieldDescriptorProto.Type.TYPE_GROUP);

        sLabelMap = new HashMap<String, DescriptorProtos.FieldDescriptorProto.Label>();
        sLabelMap.put("optional", DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        sLabelMap.put("required", DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED);
        sLabelMap.put("repeated", DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
    }
}
