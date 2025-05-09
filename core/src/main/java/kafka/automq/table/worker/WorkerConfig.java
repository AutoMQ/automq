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

import kafka.cluster.Partition;
import kafka.log.UnifiedLog;

import org.apache.kafka.server.record.TableTopicSchemaType;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.List;

import static kafka.automq.table.utils.PartitionUtil.stringToList;

public class WorkerConfig {
    static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

    private final UnifiedLog log;
    private LogConfig config;

    public WorkerConfig(Partition partition) {
        this.log = partition.log().get();
        this.config = log.config();
    }

    // for testing
    public WorkerConfig() {
        this.log = null;
    }

    public String namespace() {
        return config.tableTopicNamespace;
    }

    public TableTopicSchemaType schemaType() {
        return config.tableTopicSchemaType;
    }

    public long incrementSyncThreshold() {
        return 32 * 1024 * 1024;
    }

    public int microSyncBatchSize() {
        return 32 * 1024 * 1024;
    }

    public List<String> idColumns() {
        String str = config.tableTopicIdColumns;
        return stringToList(str, COMMA_NO_PARENS_REGEX);
    }

    public String partitionByConfig() {
        return config.tableTopicPartitionBy;
    }

    public List<String> partitionBy() {
        String str = config.tableTopicPartitionBy;
        return stringToList(str, COMMA_NO_PARENS_REGEX);
    }

    public boolean upsertEnable() {
        return config.tableTopicUpsertEnable;
    }

    public String cdcField() {
        return config.tableTopicCdcField;
    }

    public void refresh() {
        this.config = log.config();
    }


}
