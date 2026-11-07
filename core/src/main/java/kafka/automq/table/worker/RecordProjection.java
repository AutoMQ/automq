/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package kafka.automq.table.worker;

import com.google.common.base.Preconditions;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

import java.util.List;
import java.util.Map;

/**
 * This is modified from {@link org.apache.iceberg.util.StructProjection} to support record types.
 */
public class RecordProjection implements Record {

    /**
     * Creates a projecting wrapper for {@link Record} rows.
     *
     * <p>This projection does not work with repeated types like lists and maps.
     *
     * @param dataSchema      schema of rows wrapped by this projection
     * @param projectedSchema result schema of the projected rows
     * @return a wrapper to project rows
     */
    public static RecordProjection create(Schema dataSchema, Schema projectedSchema) {
        return new RecordProjection(dataSchema.asStruct(), projectedSchema.asStruct());
    }

    private final StructType type;
    private final int[] positionMap;
    private final RecordProjection[] nestedProjections;
    private Record record;

    private RecordProjection(StructType structType, StructType projection) {
        this(structType, projection, false);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private RecordProjection(StructType structType, StructType projection, boolean allowMissing) {
        this.type = projection;
        this.positionMap = new int[projection.fields().size()];
        this.nestedProjections = new RecordProjection[projection.fields().size()];

        // set up the projection positions and any nested projections that are needed
        List<NestedField> dataFields = structType.fields();
        for (int pos = 0; pos < positionMap.length; pos += 1) {
            Types.NestedField projectedField = projection.fields().get(pos);

            boolean found = false;
            for (int i = 0; !found && i < dataFields.size(); i += 1) {
                Types.NestedField dataField = dataFields.get(i);
                if (projectedField.fieldId() == dataField.fieldId()) {
                    found = true;
                    positionMap[pos] = i;
                    switch (projectedField.type().typeId()) {
                        case STRUCT:
                            nestedProjections[pos] =
                                new RecordProjection(
                                    dataField.type().asStructType(), projectedField.type().asStructType());
                            break;
                        case MAP:
                            MapType projectedMap = projectedField.type().asMapType();
                            MapType originalMap = dataField.type().asMapType();

                            boolean keyProjectable =
                                !projectedMap.keyType().isNestedType()
                                    || projectedMap.keyType().equals(originalMap.keyType());
                            boolean valueProjectable =
                                !projectedMap.valueType().isNestedType()
                                    || projectedMap.valueType().equals(originalMap.valueType());
                            Preconditions.checkArgument(
                                keyProjectable && valueProjectable,
                                "Cannot project a partial map key or value struct. Trying to project %s out of %s",
                                projectedField,
                                dataField);

                            nestedProjections[pos] = null;
                            break;
                        case LIST:
                            ListType projectedList = projectedField.type().asListType();
                            ListType originalList = dataField.type().asListType();

                            boolean elementProjectable =
                                !projectedList.elementType().isNestedType()
                                    || projectedList.elementType().equals(originalList.elementType());
                            Preconditions.checkArgument(
                                elementProjectable,
                                "Cannot project a partial list element struct. Trying to project %s out of %s",
                                projectedField,
                                dataField);

                            nestedProjections[pos] = null;
                            break;
                        default:
                            nestedProjections[pos] = null;
                    }
                }
            }

            if (!found && projectedField.isOptional() && allowMissing) {
                positionMap[pos] = -1;
                nestedProjections[pos] = null;
            } else if (!found) {
                throw new IllegalArgumentException(
                    String.format("Cannot find field %s in %s", projectedField, structType));
            }
        }
    }

    public RecordProjection wrap(Record newRecord) {
        this.record = newRecord;
        return this;
    }

    @Override
    public int size() {
        return type.fields().size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        // struct can be null if wrap is not called first before the get call
        // or if a null struct is wrapped.
        if (record == null) {
            return null;
        }

        int recordPos = positionMap[pos];
        if (nestedProjections[pos] != null) {
            Record nestedStruct = record.get(recordPos, Record.class);
            if (nestedStruct == null) {
                return null;
            }

            return javaClass.cast(nestedProjections[pos].wrap(nestedStruct));
        }

        if (recordPos != -1) {
            return record.get(recordPos, javaClass);
        } else {
            return null;
        }
    }

    @Override
    public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StructType struct() {
        return type;
    }

    @Override
    public Object getField(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setField(String name, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int pos) {
        return get(pos, Object.class);
    }

    @Override
    public Record copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        throw new UnsupportedOperationException();
    }
}
