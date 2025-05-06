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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

    private final Schema schema;
    private final Schema deleteSchema;
    private final InternalRecordWrapper wrapper;
    private final InternalRecordWrapper keyWrapper;
    private final RecordProjection keyProjection;

    BaseDeltaTaskWriter(
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema,
        Set<Integer> identifierFieldIds
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, new HashSet<>(identifierFieldIds));
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
        this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
        this.keyProjection = RecordProjection.create(schema, deleteSchema);
    }

    abstract RowDataDeltaWriter partition(Record row);

    InternalRecordWrapper wrapper() {
        return wrapper;
    }

    @Override
    public void write(Record row) throws IOException {
        Operation op = ((RecordWrapper) row).op();
        RowDataDeltaWriter writer = partition(row);
        if (op == Operation.UPDATE || op == Operation.DELETE) {
            writer.deleteKey(keyProjection.wrap(row));
        }
        if (op == Operation.UPDATE || op == Operation.INSERT) {
            writer.write(row);
        }
    }

    class RowDataDeltaWriter extends BaseEqualityDeltaWriter {

        RowDataDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(Record data) {
            return keyWrapper.wrap(data);
        }
    }
}
