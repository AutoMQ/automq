/*
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
package org.apache.kafka.server.config;

public class Defaults {

    /** ********* Kafka on S3 Configuration *********/
    public static final int S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL = 20; // 20min
    public static final long S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE = 200 * 1024 * 1024; // 200MB
    public static final long S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE = 8 * 1024 * 1024; // 8MB
    public static final int S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES = 120; // 120min
    public static final int S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM = 500;
    public static final int S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT = 100000;
    public static final int S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT = 10000;
    public static final long S3_OBJECT_DELETE_RETENTION_MINUTES = 5; // 5min
    public static final long S3_NETWORK_BASELINE_BANDWIDTH = 100 * 1024 * 1024; // 100MB/s
    public static final int S3_REFILL_PERIOD_MS = 10; // 10ms
    public static final int S3_METRICS_EXPORTER_REPORT_INTERVAL_MS = 30000; // 30s
    public static final int S3_SPAN_SCHEDULED_DELAY_MS = 1000; // 1s
    public static final int S3_SPAN_MAX_QUEUE_SIZE = 5120;
    public static final int S3_SPAN_MAX_BATCH_SIZE = 1024;
    public static final String S3_EXPORTER_OTLPPROTOCOL = "grpc";
}
