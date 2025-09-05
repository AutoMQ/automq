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

package kafka.automq.table.worker;

import kafka.automq.table.process.DefaultRecordProcessor;
import kafka.automq.table.process.convert.RawConverter;
import kafka.automq.table.process.exception.RecordProcessorException;

import org.apache.kafka.common.record.Record;

import com.google.common.collect.ImmutableMap;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Tag("S3Unit")
public class MemoryWriter extends IcebergWriter {
    List<Record> records = new LinkedList<>();

    public MemoryWriter(WorkerConfig config) {
        super(new IcebergTableManager(catalog(), TableIdentifier.parse("default.test"), config), new DefaultRecordProcessor("test", new RawConverter(), new RawConverter()), config);
    }

    @Override
    protected boolean write0(int partition, Record kafkaRecord) throws IOException, RecordProcessorException {
        super.write0(partition, kafkaRecord);
        records.add(kafkaRecord);
        return true;
    }

    private static Catalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
        return catalog;
    }
}
