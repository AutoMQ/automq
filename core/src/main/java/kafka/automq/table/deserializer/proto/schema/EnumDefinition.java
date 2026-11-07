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

public class EnumDefinition {
    // --- public static ---

    public static Builder newBuilder(String enumName) {
        return newBuilder(enumName, null);
    }

    public static Builder newBuilder(String enumName, Boolean allowAlias) {
        return new Builder(enumName, allowAlias);
    }

    // --- public ---

    public String toString() {
        return mEnumType.toString();
    }

    // --- package ---

    DescriptorProtos.EnumDescriptorProto getEnumType() {
        return mEnumType;
    }

    // --- private ---

    private EnumDefinition(DescriptorProtos.EnumDescriptorProto enumType) {
        mEnumType = enumType;
    }

    private DescriptorProtos.EnumDescriptorProto mEnumType;

    /**
     * EnumDefinition.Builder
     */
    public static class Builder {
        // --- public ---

        public Builder addValue(String name, int num) {
            DescriptorProtos.EnumValueDescriptorProto.Builder enumValBuilder = DescriptorProtos.EnumValueDescriptorProto
                    .newBuilder();
            enumValBuilder.setName(name).setNumber(num);
            mEnumTypeBuilder.addValue(enumValBuilder.build());
            return this;
        }

        public EnumDefinition build() {
            return new EnumDefinition(mEnumTypeBuilder.build());
        }

        // --- private ---

        private Builder(String enumName, Boolean allowAlias) {
            mEnumTypeBuilder = DescriptorProtos.EnumDescriptorProto.newBuilder();
            mEnumTypeBuilder.setName(enumName);
            if (allowAlias != null) {
                DescriptorProtos.EnumOptions.Builder optionsBuilder = DescriptorProtos.EnumOptions
                        .newBuilder();
                optionsBuilder.setAllowAlias(allowAlias);
                mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
            }
        }

        private DescriptorProtos.EnumDescriptorProto.Builder mEnumTypeBuilder;
    }
}
