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

package kafka.automq.table.events;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Tag;

import java.util.Map;
import java.util.UUID;

@Tag("S3Unit")
public class EventTestUtil {
    public static DataFile createDataFile() {
        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .withMetrics(new Metrics(1L, Map.of(1, 2L, 3, 4L), null, null, null))
            .build();
    }

    public static DeleteFile createDeleteFile() {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(1)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    }
}