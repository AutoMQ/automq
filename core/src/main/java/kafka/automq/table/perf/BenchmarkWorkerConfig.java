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

package kafka.automq.table.perf;

import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.server.record.ErrorsTolerance;
import org.apache.kafka.server.record.TableTopicConvertType;
import org.apache.kafka.server.record.TableTopicSchemaType;
import org.apache.kafka.server.record.TableTopicTransformType;

import java.util.Collections;
import java.util.List;

class BenchmarkWorkerConfig extends WorkerConfig {

    public BenchmarkWorkerConfig() {
        super();
    }

    @Override
    public String namespace() {
        return "test";
    }

    @Override
    public TableTopicSchemaType schemaType() {
        return TableTopicSchemaType.NONE;
    }

    @Override
    public TableTopicConvertType valueConvertType() {
        return TableTopicConvertType.BY_SCHEMA_ID;
    }

    @Override
    public TableTopicConvertType keyConvertType() {
        return TableTopicConvertType.STRING;
    }

    @Override
    public TableTopicTransformType transformType() {
        return TableTopicTransformType.FLATTEN;
    }

    @Override
    public String valueSubject() {
        return null;
    }

    @Override
    public String valueMessageFullName() {
        return null;
    }

    @Override
    public String keySubject() {
        return null;
    }

    @Override
    public String keyMessageFullName() {
        return null;
    }

    @Override
    public List<String> idColumns() {
        return Collections.emptyList();
    }

    @Override
    public String partitionByConfig() {
        return null;
    }

    @Override
    public List<String> partitionBy() {
        return Collections.emptyList();
    }

    @Override
    public boolean upsertEnable() {
        return false;
    }

    @Override
    public ErrorsTolerance errorsTolerance() {
        return ErrorsTolerance.ALL;
    }

    @Override
    public String cdcField() {
        return null;
    }
}
