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

import kafka.automq.table.transformer.ConverterFactory;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergWriterFactory implements WriterFactory {
    private final TableIdentifier tableIdentifier;
    private final IcebergTableManager icebergTableManager;
    private final ConverterFactory converterFactory;
    private final WorkerConfig config;
    private final String topic;

    public IcebergWriterFactory(Catalog catalog, TableIdentifier tableIdentifier, ConverterFactory converterFactory, WorkerConfig config, String topic) {
        this.topic = topic;
        this.tableIdentifier = tableIdentifier;
        this.icebergTableManager = new IcebergTableManager(catalog, tableIdentifier, config);
        this.converterFactory = converterFactory;
        this.config = config;
    }

    @Override
    public Writer newWriter() {
        return new IcebergWriter(icebergTableManager, converterFactory.converter(config.schemaType(), topic), config);
    }

    @Override
    public PartitionSpec partitionSpec() {
        return icebergTableManager.spec();
    }

    @Override
    public void reset() {
        icebergTableManager.reset();
    }
}
