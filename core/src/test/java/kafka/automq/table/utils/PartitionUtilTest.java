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

package kafka.automq.table.utils;

import com.google.common.collect.ImmutableMap;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;


@Tag("S3Unit")
public class PartitionUtilTest {

    @Test
    public void testEvolve() {
        InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize("test", ImmutableMap.of());
        inMemoryCatalog.createNamespace(Namespace.of("default"));
        Schema v1Schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "timestamp", Types.TimestampType.withZone())
        );
        Table table = inMemoryCatalog.createTable(TableIdentifier.parse("default.test"), v1Schema);
        // add partition
        PartitionUtil.evolve(List.of("id", "bucket(name, 8)", "hour(timestamp)"), table);
        Assertions.assertEquals(List.of("id", "name_bucket_8", "timestamp_hour"), table.spec().fields().stream().map(PartitionField::name).toList());

        // replace partition
        PartitionUtil.evolve(List.of("id", "bucket(name, 8)", "day(timestamp)"), table);
        Assertions.assertEquals(List.of("id", "name_bucket_8", "timestamp_day"), table.spec().fields().stream().map(PartitionField::name).toList());

        // drop partition
        PartitionUtil.evolve(List.of("bucket(name, 8)", "day(timestamp)"), table);
        Assertions.assertEquals(List.of("name_bucket_8", "timestamp_day"), table.spec().fields().stream().map(PartitionField::name).toList());
    }

}
