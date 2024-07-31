/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq;

import com.automq.stream.s3.ByteBufAllocPolicy;
import com.automq.stream.s3.operator.BucketURI;
import java.util.ArrayList;
import java.util.List;
import kafka.log.stream.s3.telemetry.exporter.ExporterConstants;
import kafka.server.KafkaConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class AutoMQConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoMQConfig.class);

    public static final String ELASTIC_STREAM_ENABLE_CONFIG = "elasticstream.enable";
    public static final String ELASTIC_STREAM_ENABLE_DOC = "Whether to enable AutoMQ, it has to be set to true";

    public static final String ELASTIC_STREAM_ENDPOINT_CONFIG = "elasticstream.endpoint";
    public static final String ELASTIC_STREAM_ENDPOINT_DOC = "Specifies the Elastic Stream endpoint, ex. <code>es://hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
        "You could also PoC launch it in memory mode with endpoint <code>memory:://</code> or redis mode with <code>redis://.</code>";

    public static final String ELASTIC_STREAM_NAMESPACE_CONFIG = "elasticstream.namespace";
    public static final String ELASTIC_STREAM_NAMESPACE_DOC = "The kafka cluster in which elastic stream namespace which should conflict with other kafka cluster sharing the same elastic stream.";

    public static final String S3_DATA_BUCKETS_CONFIG = "s3.data.buckets";
    public static final String S3_DATA_BUCKETS_DOC = "The data buckets url with format 0@s3://$bucket?region=$region. \n" +
        "the full url format for s3 is 0@s3://$bucket?region=$region[&endpoint=$endpoint][&pathStyle=$enablePathStyle][&authType=$authType][&accessKey=$accessKey][&secretKey=$secretKey]" +
        "- pathStyle: true|false. The object storage access path style. When using MinIO, it should be set to true.\n" +
        "- authType: instance|static. When set to instance, it will use instance profile to auth. When set to static, it will get accessKey and secretKey from the url or from system environment KAFKA_S3_ACCESS_KEY/KAFKA_S3_SECRET_KEY.";

    public static final String S3_OPS_BUCKETS_CONFIG = "s3.ops.buckets";
    public static final String S3_OPS_BUCKETS_DOC = "With the same format as s3.data.buckets";

    public static final String S3_WAL_PATH_CONFIG = "s3.wal.path";
    public static final String S3_WAL_PATH_DOC = "The local WAL path for AutoMQ can be set to a block device path such as 0@file:///dev/xxx?iops=3000&iodepth=8 or a filesystem file path." +
        "It is recommended to use a block device for better write performance.";

    public static final String S3_WAL_CACHE_SIZE_CONFIG = "s3.wal.cache.size";
    public static final String S3_WAL_CACHE_SIZE_DOC = "The WAL (Write-Ahead Log) cache is a FIFO (First In, First Out) queue that contains data that has not yet been uploaded to object storage, as well as data that has been uploaded but not yet evicted from the cache." +
        "When the data in the cache that has not been uploaded fills the entire capacity, the storage will backpressure subsequent requests until the data upload is completed." +
        "It will be set to a reasonable value according to memory by default.";

    public static final String S3_WAL_UPLOAD_THRESHOLD_CONFIG = "s3.wal.upload.threshold";
    public static final String S3_WAL_UPLOAD_THRESHOLD_DOC = "The threshold at which WAL triggers upload to object storage. The configuration value needs to be less than s3.wal.cache.size. The larger the configuration value, the higher the data aggregation and the lower the cost of metadata storage.";

    public static final String S3_STREAM_SPLIT_SIZE_CONFIG = "s3.stream.object.split.size";
    public static final String S3_STREAM_SPLIT_SIZE_DOC = "The S3 stream object split size threshold when upload delta WAL or compact stream set object.";

    public static final String S3_OBJECT_BLOCK_SIZE_CONFIG = "s3.object.block.size";
    public static final String S3_OBJECT_BLOCK_SIZE_DOC = "The S3 object compressed block size threshold.";

    public static final String S3_OBJECT_PART_SIZE_CONFIG = "s3.object.part.size";
    public static final String S3_OBJECT_PART_SIZE_DOC = "The S3 object multi-part upload part size threshold.";

    public static final String S3_BLOCK_CACHE_SIZE_CONFIG = "s3.block.cache.size";
    public static final String S3_BLOCK_CACHE_SIZE_DOC = "s3.block.cache.size is the size of the block cache. The block cache is used to cache cold data read from object storage. ";

    public static final String S3_STREAM_ALLOCATOR_POLICY_CONFIG = "s3.stream.allocator.policy";
    public static final String S3_STREAM_ALLOCATOR_POLICY_DOC = "The S3 stream memory allocator policy, supported value: " + ByteBufAllocPolicy.values() + ".\n" +
        "Please note that when configured to use DIRECT memory, it is necessary to modify the heap size (e.g., -Xmx) and the direct memory size (e.g., -XX:MaxDirectMemorySize) in the vm options." +
        " You can set them through the environment variable KAFKA_HEAP_OPTS.";

    public static final String S3_STREAM_OBJECT_COMPACTION_INTERVAL_MINUTES_CONFIG = "s3.stream.object.compaction.interval.minutes";
    public static final String S3_STREAM_OBJECT_COMPACTION_INTERVAL_MINUTES_DOC = "Interpublic static final String period for stream object compaction. The larger the interval, the lower the cost of API calls, but it increases the scale of metadata storage.";

    public static final String S3_STREAM_OBJECT_COMPACTION_MAX_SIZE_BYTES_CONFIG = "s3.stream.object.compaction.max.size.bytes";
    public static final String S3_STREAM_OBJECT_COMPACTION_MAX_SIZE_BYTES_DOC = "The maximum size of the object that Stream object compaction allows to synthesize. The larger this value, the higher the cost of API calls, but the smaller the scale of metadata storage.";

    public static final String S3_CONTROLLER_REQUEST_RETRY_MAX_COUNT_CONFIG = "s3.controller.request.retry.max.count";
    public static final String S3_CONTROLLER_REQUEST_RETRY_MAX_COUNT_DOC = "The S3 controller request retry max count.";

    public static final String S3_CONTROLLER_REQUEST_RETRY_BASE_DELAY_MS_CONFIG = "s3.controller.request.retry.base.delay.ms";
    public static final String S3_CONTROLLER_REQUEST_RETRY_BASE_DELAY_MS_DOC = "The S3 controller request retry base delay in milliseconds.";

    public static final String S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL_CONFIG = "s3.stream.set.object.compaction.interval.minutes";
    public static final String S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL_DOC = "Set the interpublic static final String for Stream object compaction. The smaller this value, the smaller the scale of metadata storage, and the earlier the data can become compact. " +
        "However, the number of compactions that the final generated stream object goes through will increase.";
    public static final int S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL = 10; // 10min

    public static final String S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE_CONFIG = "s3.stream.set.object.compaction.cache.size";
    public static final String S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE_DOC = "The size of memory is available during the Stream object compaction process. The larger this value, the lower the cost of API calls.";
    public static final long S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE = 200 * 1024 * 1024; // 200MB

    public static final String S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE_CONFIG = "s3.stream.set.object.compaction.stream.split.size";
    public static final String S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE_DOC = "During the Stream object compaction process, if the amount of data in a single stream exceeds this threshold, the data of this stream will be directly split and written into a single stream object. " +
        "The smaller this value, the earlier the data can be split from the stream set object, the lower the subsequent API call cost for stream object compaction, but the higher the API call cost for splitting.";
    public static final long S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE = 8 * 1024 * 1024; // 8MB

    public static final String S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES_CONFIG = "s3.stream.set.object.compaction.force.split.minutes";
    public static final String S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES_DOC = "The stream set object compaction force split period in minutes.";
    public static final int S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES = 120; // 120min

    public static final String S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM_CONFIG = "s3.stream.set.object.compaction.max.num";
    public static final String S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM_DOC = "The maximum number of stream set objects to be compact at one time.";
    public static final int S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM = 500;

    public static final String S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT_CONFIG = "s3.max.stream.num.per.stream.set.object";
    public static final String S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT_DOC = "The maximum number of streams allowed in single stream set object";
    public static final int S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT = 100000;

    public static final String S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT_CONFIG = "s3.max.stream.object.num.per.commit";
    public static final String S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT_DOC = "The maximum number of stream objects in single commit request";
    public static final int S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT = 10000;

    public static final String S3_MOCK_ENABLE_CONFIG = "s3.mock.enable";
    public static final String S3_MOCK_ENABLE_DOC = "The S3 mock enable flag, replace all S3 related module with memory-mocked implement.";

    public static final String S3_OBJECT_DELETION_MINUTES_CONFIG = "s3.object.delete.retention.minutes";
    public static final String S3_OBJECT_DELETION_MINUTES_DOC = "The marked-for-deletion S3 object retention time in minutes, default is 10 minutes (600s).";
    public static final long S3_OBJECT_DELETE_RETENTION_MINUTES = 1; // 1min

    public static final String S3_OBJECT_LOG_ENABLE_CONFIG = "s3.object.log.enable";
    public static final String S3_OBJECT_LOG_ENABLE_DOC = "Whether to enable S3 object trace log.";

    public static final String S3_NETWORK_BASELINE_BANDWIDTH_CONFIG = "s3.network.baseline.bandwidth";
    public static final String S3_NETWORK_BASELINE_BANDWIDTH_DOC = "The total available bandwidth for object storage requests. This is used to prevent stream set object compaction and catch-up read from monopolizing normal read and write traffic. Produce and Consume will also separately consume traffic in and traffic out. " +
        "For example, suppose this value is set to 100MB/s, and the normal read and write traffic is 80MB/s, then the available traffic for stream set object compaction is 20MB/s.";
    public static final long S3_NETWORK_BASELINE_BANDWIDTH = 100 * 1024 * 1024; // 100MB/s

    public static final String S3_NETWORK_REFILL_PERIOD_MS_CONFIG = "s3.network.refill.period.ms";
    public static final String S3_NETWORK_REFILL_PERIOD_MS_DOC = "The network bandwidth token refill period in milliseconds.";
    public static final int S3_REFILL_PERIOD_MS = 10; // 10ms

    public static final String S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG = "s3.telemetry.metrics.exporter.uri";
    public static final String S3_TELEMETRY_METRICS_EXPORTER_URI_DOC = "The metrics exporter uri, format: $type://?$param1=$value1&$param2=$value2. " +
        "Valid types: prometheus, otlp, ops. Multiple URIs of different types can be configured together with comma as separator. " +
        "URI format for type prometheus: prometheus://?host=$hostname&port=$port" +
        " - host: the host address of the built-in Prometheus HTTP server that used to expose the OTel metrics. Default: localhost" +
        " - port: the port number of the built-in Prometheus HTTP server that used to expose the OTel metrics. Default: 9090" +
        "URI format for type otlp: otlp://?endpoint=$endpoint&protocol=$protocol&compression=$compression" +
        " - endpoint: the endpoint to push metrics to, e.g. http://localhost:4317" +
        " - protocol: the protocol to use when exporting metrics to endpoint, valid values: grpc, http. Default: grpc" +
        " - compression: compression type, value values: gzip, none. Default: none" +
        "URI format for type ops: ops://?" +
        " - when configured, metrics will be exported to S3 with bucket specified by s3.ops.buckets configuration";

    public static final String S3_TELEMETRY_EXPORTER_REPORT_INTERVAL_MS_CONFIG = "s3.telemetry.exporter.report.interval.ms";
    public static final String S3_TELEMETRY_EXPORTER_REPORT_INTERVAL_MS_DOC = "This configuration controls how often the metrics should be exported.";
    public static final int S3_METRICS_EXPORTER_REPORT_INTERVAL_MS = 30000; // 30s

    public static final String S3_TELEMETRY_METRICS_LEVEL_CONFIG = "s3.telemetry.metrics.level";
    public static final String S3_TELEMETRY_METRICS_LEVEL_DOC = "The metrics level that will be used on recording metrics. The \"INFO\" level includes most of the metrics that users should care about, for example throughput and latency of common stream operations. " +
        "The \"DEBUG\" level includes detailed metrics that would help with diagnosis, for example latency of different stages when writing to underlying block device.";

    public static final String CLUSTER_ID_CONFIG = "cluster.id";
    public static final String CLUSTER_ID_DOC = "If the cluster.id is set, Kafka will auto format the storage.";

    // Deprecated config start
    public static final String S3_ENDPOINT_CONFIG = "s3.endpoint";
    public static final String S3_ENDPOINT_DOC = "[DEPRECATED]please use s3.data.buckets. The object storage endpoint, ex. <code>https://s3.us-east-1.amazonaws.com</code>.";

    public static final String S3_REGION_CONFIG = "s3.region";
    public static final String S3_REGION_DOC = "[DEPRECATED]please use s3.data.buckets. The object storage region, ex. <code>us-east-1</code>.";

    public static final String S3_PATH_STYLE_CONFIG = "s3.path.style";
    public static final String S3_PATH_STYLE_DOC = "[DEPRECATED]please use s3.data.buckets. The object storage access path style. When using MinIO, it should be set to true.";

    public static final String S3_BUCKET_CONFIG = "s3.bucket";
    public static final String S3_BUCKET_DOC = "[DEPRECATED]please use s3.data.buckets. The object storage bucket.";

    public static final String S3_OPS_BUCKET_CONFIG = "s3.ops.bucket";
    public static final String S3_OPS_BUCKET_DOC = "[DEPRECATED]please use s3.ops.buckets. The object storage ops bucket.";

    public static final String S3_WAL_CAPACITY_CONFIG = "s3.wal.capacity";
    public static final String S3_WAL_CAPACITY_DOC = "[DEPRECATED]please use s3.wal.path. The size of the local WAL for AutoMQ. This determines the maximum amount of data that can be written to the buffer before data is uploaded to object storage." +
        "A larger capacity can tolerate more write jitter in object storage.";

    public static final String S3_WAL_THREAD_CONFIG = "s3.wal.thread";
    public static final String S3_WAL_THREAD_DOC = "[DEPRECATED]please use s3.wal.path. The IO thread count for S3 WAL.";

    public static final String S3_WAL_IOPS_CONFIG = "s3.wal.iops";
    public static final String S3_WAL_IOPS_DOC = "[DEPRECATED]please use s3.wal.path. The max iops for S3 WAL.";

    public static final String S3_METRICS_ENABLE_CONFIG = "s3.telemetry.metrics.enable";
    public static final String S3_METRICS_ENABLE_DOC = "[DEPRECATED] use s3.metrics.uri instead.";

    public static final String S3_TELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG = "s3.telemetry.exporter.otlp.endpoint";
    public static final String S3_TELEMETRY_EXPORTER_OTLP_ENDPOINT_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead";

    public static final String S3_TELEMETRY_EXPORTER_OTLP_PROTOCOL_CONFIG = "s3.telemetry.exporter.otlp.protocol";
    public static final String S3_TELEMETRY_EXPORTER_OTLP_PROTOCOL_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead. The protocol to use when using OTLP exporter.";
    public static final String S3_EXPORTER_OTLPPROTOCOL = "grpc";

    public static final String S3_TELEMETRY_EXPORTER_OTLP_COMPRESSION_ENABLE_CONFIG = "s3.telemetry.exporter.otlp.compression.enable";
    public static final String S3_TELEMETRY_EXPORTER_OTLP_COMPRESSION_ENABLE_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead. Whether to enable compression for OTLP exporter, valid only when use http protocol.";

    public static final String S3_TELEMETRY_METRICS_EXPORTER_TYPE_CONFIG = "s3.telemetry.metrics.exporter.type";
    public static final String S3_TELEMETRY_METRICS_EXPORTER_TYPE_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead. The list of metrics exporter types that should be used. The \"otlp\" type will export metrics to backend service with OTLP protocol. The \"prometheus\" type will start a built-in HTTP server that allows Prometheus backend scrape metrics from it.";

    public static final String S3_METRICS_EXPORTER_PROM_HOST_CONFIG = "s3.metrics.exporter.prom.host";
    public static final String S3_METRICS_EXPORTER_PROM_HOST_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead. The host address of the built-in Prometheus HTTP server that used to expose the OTel metrics.";

    public static final String S3_METRICS_EXPORTER_PROM_PORT_CONFIG = "s3.metrics.exporter.prom.port";
    public static final String S3_METRICS_EXPORTER_PROM_PORT_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead. The port number of the built-in Prometheus HTTP server that used to expose the OTel metrics.";

    public static final String S3_TELEMETRY_OPS_ENABLED_CONFIG = "s3.telemetry.ops.enabled";
    public static final String S3_TELEMETRY_OPS_ENABLED_DOC = "[DEPRECATED] use s3.telemetry.metrics.uri instead.";
    // Deprecated config end

    public static void define(ConfigDef configDef) {
        configDef.define(AutoMQConfig.ELASTIC_STREAM_ENABLE_CONFIG, BOOLEAN, false, HIGH, AutoMQConfig.ELASTIC_STREAM_ENABLE_DOC)
            .define(AutoMQConfig.ELASTIC_STREAM_ENDPOINT_CONFIG, STRING, "s3://", HIGH, AutoMQConfig.ELASTIC_STREAM_ENDPOINT_DOC)
            .define(AutoMQConfig.ELASTIC_STREAM_NAMESPACE_CONFIG, STRING, null, MEDIUM, AutoMQConfig.ELASTIC_STREAM_NAMESPACE_DOC)
            .define(AutoMQConfig.S3_DATA_BUCKETS_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_DATA_BUCKETS_DOC)
            .define(AutoMQConfig.S3_OPS_BUCKETS_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_OPS_BUCKETS_DOC)
            .define(AutoMQConfig.S3_WAL_PATH_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_WAL_PATH_DOC)
            .define(AutoMQConfig.S3_WAL_CACHE_SIZE_CONFIG, LONG, -1L, MEDIUM, AutoMQConfig.S3_WAL_CACHE_SIZE_DOC)
            .define(AutoMQConfig.S3_WAL_UPLOAD_THRESHOLD_CONFIG, LONG, -1L, MEDIUM, AutoMQConfig.S3_WAL_UPLOAD_THRESHOLD_DOC)
            .define(AutoMQConfig.S3_STREAM_SPLIT_SIZE_CONFIG, INT, 8388608, MEDIUM, AutoMQConfig.S3_STREAM_SPLIT_SIZE_DOC)
            .define(AutoMQConfig.S3_OBJECT_BLOCK_SIZE_CONFIG, INT, 524288, MEDIUM, AutoMQConfig.S3_OBJECT_BLOCK_SIZE_DOC)
            .define(AutoMQConfig.S3_OBJECT_PART_SIZE_CONFIG, INT, 16777216, MEDIUM, AutoMQConfig.S3_OBJECT_PART_SIZE_DOC)
            .define(AutoMQConfig.S3_BLOCK_CACHE_SIZE_CONFIG, LONG, -1L, MEDIUM, AutoMQConfig.S3_BLOCK_CACHE_SIZE_DOC)
            .define(AutoMQConfig.S3_STREAM_ALLOCATOR_POLICY_CONFIG, STRING, ByteBufAllocPolicy.POOLED_HEAP.name(), MEDIUM, AutoMQConfig.S3_STREAM_ALLOCATOR_POLICY_DOC)
            .define(AutoMQConfig.S3_STREAM_OBJECT_COMPACTION_INTERVAL_MINUTES_CONFIG, INT, 30, MEDIUM, AutoMQConfig.S3_STREAM_OBJECT_COMPACTION_INTERVAL_MINUTES_DOC)
            .define(AutoMQConfig.S3_STREAM_OBJECT_COMPACTION_MAX_SIZE_BYTES_CONFIG, LONG, 1073741824L, MEDIUM, AutoMQConfig.S3_STREAM_OBJECT_COMPACTION_MAX_SIZE_BYTES_DOC)
            .define(AutoMQConfig.S3_CONTROLLER_REQUEST_RETRY_MAX_COUNT_CONFIG, INT, Integer.MAX_VALUE, MEDIUM, AutoMQConfig.S3_CONTROLLER_REQUEST_RETRY_MAX_COUNT_DOC)
            .define(AutoMQConfig.S3_CONTROLLER_REQUEST_RETRY_BASE_DELAY_MS_CONFIG, LONG, 500, MEDIUM, AutoMQConfig.S3_CONTROLLER_REQUEST_RETRY_BASE_DELAY_MS_DOC)
            .define(AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL_CONFIG, INT, S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL, MEDIUM, AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_INTERVAL_DOC)
            .define(AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE_CONFIG, LONG, S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE, MEDIUM, AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_CACHE_SIZE_DOC)
            .define(AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE_CONFIG, LONG, S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE, MEDIUM, AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_STREAM_SPLIT_SIZE_DOC)
            .define(AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES_CONFIG, INT, S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES, MEDIUM, AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_FORCE_SPLIT_MINUTES_DOC)
            .define(AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM_CONFIG, INT, S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM, MEDIUM, AutoMQConfig.S3_STREAM_SET_OBJECT_COMPACTION_MAX_OBJECT_NUM_DOC)
            .define(AutoMQConfig.S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT_CONFIG, INT, S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT, MEDIUM, AutoMQConfig.S3_MAX_STREAM_NUM_PER_STREAM_SET_OBJECT_DOC)
            .define(AutoMQConfig.S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT_CONFIG, INT, S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT, MEDIUM, AutoMQConfig.S3_MAX_STREAM_OBJECT_NUM_PER_COMMIT_DOC)
            .define(AutoMQConfig.S3_MOCK_ENABLE_CONFIG, BOOLEAN, false, LOW, AutoMQConfig.S3_MOCK_ENABLE_DOC)
            .define(AutoMQConfig.S3_OBJECT_DELETION_MINUTES_CONFIG, LONG, S3_OBJECT_DELETE_RETENTION_MINUTES, MEDIUM, AutoMQConfig.S3_OBJECT_DELETION_MINUTES_DOC)
            .define(AutoMQConfig.S3_OBJECT_LOG_ENABLE_CONFIG, BOOLEAN, false, LOW, AutoMQConfig.S3_OBJECT_LOG_ENABLE_DOC)
            .define(AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG, LONG, S3_NETWORK_BASELINE_BANDWIDTH, MEDIUM, AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_DOC)
            .define(AutoMQConfig.S3_NETWORK_REFILL_PERIOD_MS_CONFIG, INT, S3_REFILL_PERIOD_MS, MEDIUM, AutoMQConfig.S3_NETWORK_REFILL_PERIOD_MS_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_METRICS_LEVEL_CONFIG, STRING, "INFO", MEDIUM, AutoMQConfig.S3_TELEMETRY_METRICS_LEVEL_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_EXPORTER_REPORT_INTERVAL_MS_CONFIG, INT, S3_METRICS_EXPORTER_REPORT_INTERVAL_MS, MEDIUM, AutoMQConfig.S3_TELEMETRY_EXPORTER_REPORT_INTERVAL_MS_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_DOC)
            // Deprecated config start
            .define(AutoMQConfig.S3_ENDPOINT_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_ENDPOINT_DOC)
            .define(AutoMQConfig.S3_REGION_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_REGION_DOC)
            .define(AutoMQConfig.S3_PATH_STYLE_CONFIG, BOOLEAN, false, LOW, AutoMQConfig.S3_PATH_STYLE_DOC)
            .define(AutoMQConfig.S3_BUCKET_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_BUCKET_DOC)
            .define(AutoMQConfig.S3_OPS_BUCKET_CONFIG, STRING, null, HIGH, AutoMQConfig.S3_OPS_BUCKET_DOC)
            .define(AutoMQConfig.S3_WAL_CAPACITY_CONFIG, LONG, 2147483648L, MEDIUM, AutoMQConfig.S3_WAL_CAPACITY_DOC)
            .define(AutoMQConfig.S3_WAL_THREAD_CONFIG, INT, 8, MEDIUM, AutoMQConfig.S3_WAL_THREAD_DOC)
            .define(AutoMQConfig.S3_WAL_IOPS_CONFIG, INT, 3000, MEDIUM, AutoMQConfig.S3_WAL_IOPS_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_OPS_ENABLED_CONFIG, BOOLEAN, true, HIGH, AutoMQConfig.S3_TELEMETRY_OPS_ENABLED_DOC)
            .define(AutoMQConfig.S3_METRICS_ENABLE_CONFIG, BOOLEAN, true, MEDIUM, AutoMQConfig.S3_METRICS_ENABLE_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_TYPE_CONFIG, STRING, null, MEDIUM, AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_TYPE_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG, STRING, null, MEDIUM, AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_ENDPOINT_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_PROTOCOL_CONFIG, STRING, S3_EXPORTER_OTLPPROTOCOL, MEDIUM, AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_PROTOCOL_DOC)
            .define(AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_COMPRESSION_ENABLE_CONFIG, BOOLEAN, false, MEDIUM, AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_COMPRESSION_ENABLE_DOC)
            .define(AutoMQConfig.S3_METRICS_EXPORTER_PROM_HOST_CONFIG, STRING, "localhost", MEDIUM, AutoMQConfig.S3_METRICS_EXPORTER_PROM_HOST_DOC)
            .define(AutoMQConfig.S3_METRICS_EXPORTER_PROM_PORT_CONFIG, INT, 9090, MEDIUM, AutoMQConfig.S3_METRICS_EXPORTER_PROM_PORT_DOC);
    }

    private List<BucketURI> dataBuckets;
    private List<BucketURI> opsBuckets;
    private String walConfig;
    private String metricsExporterURI;

    public AutoMQConfig setup(KafkaConfig config) {
        dataBuckets = genDataBuckets(config);
        opsBuckets = genOpsBuckets(config);
        walConfig = genWALConfig(config);
        metricsExporterURI = genMetricsExporterURI(config);
        return this;
    }

    public List<BucketURI> dataBuckets() {
        return dataBuckets;
    }

    public List<BucketURI> opsBuckets() {
        return opsBuckets;
    }

    public String metricsExporterURI() {
        return metricsExporterURI;
    }

    public String walConfig() {
        return walConfig;
    }

    private static List<BucketURI> genDataBuckets(KafkaConfig config) {
        String dataBuckets = config.getString(S3_DATA_BUCKETS_CONFIG);
        String oldEndpoint = config.getString(S3_ENDPOINT_CONFIG);
        String oldRegion = config.getString(S3_REGION_CONFIG);
        String oldBucket = config.getString(S3_BUCKET_CONFIG);
        boolean oldPathStyle = config.getBoolean(S3_PATH_STYLE_CONFIG);
        boolean oldConfigNotExist = StringUtils.isAllBlank(oldEndpoint, oldRegion, oldBucket);
        if (!oldConfigNotExist && StringUtils.isNotBlank(dataBuckets)) {
            LOGGER.warn("The s3.endpoint, s3.region, s3.bucket and s3.path.style configs are deprecated. The new s3.data.buckets config should be used.");
        }
        if (StringUtils.isBlank(dataBuckets)) {
            dataBuckets = "0@s3://" + oldBucket + "?region=" + oldRegion + "&endpoint=" + oldEndpoint + "&pathStyle=" + oldPathStyle;
        }
        return BucketURI.parseBuckets(dataBuckets);
    }

    private static List<BucketURI> genOpsBuckets(KafkaConfig config) {
        String opsBuckets = config.getString(S3_OPS_BUCKETS_CONFIG);
        String oldOpsBucket = config.getString(S3_OPS_BUCKET_CONFIG);
        if (StringUtils.isNotBlank(oldOpsBucket)) {
            LOGGER.warn("The s3.ops.bucket config is deprecated. The new s3.ops.buckets config should be used.");
        }
        return BucketURI.parseBuckets(opsBuckets);
    }

    private static String genWALConfig(KafkaConfig config) {
        String walConfig = config.getString(S3_WAL_PATH_CONFIG);
        if (walConfig == null) {
            return null;
        }
        if (walConfig.contains("@")) {
            return walConfig;
        }
        walConfig = String.format("0@file://%s?capacity=%d&iops=%d&iodepth=%d",
            walConfig,
            config.getLong(S3_WAL_CAPACITY_CONFIG),
            config.getInt(S3_WAL_IOPS_CONFIG),
            config.getInt(S3_WAL_THREAD_CONFIG));
        return walConfig;
    }

    private static String genMetricsExporterURI(KafkaConfig config) {
        String uri = config.getString(S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG);
        if (uri == null) {
            uri = buildMetrixExporterURIWithOldConfigs(config);
        }
        return uri;
    }

    private static String buildMetrixExporterURIWithOldConfigs(KafkaConfig kafkaConfig) {
        if (!kafkaConfig.getBoolean(S3_METRICS_ENABLE_CONFIG)) {
            return null;
        }
        List<String> exportedUris = new ArrayList<>();
        String exporterTypes = kafkaConfig.getString(S3_TELEMETRY_METRICS_EXPORTER_TYPE_CONFIG);
        if (!StringUtils.isBlank(exporterTypes)) {
            String[] exporterTypeArray = exporterTypes.split(",");
            for (String exporterType : exporterTypeArray) {
                exporterType = exporterType.trim();
                switch (exporterType) {
                    case ExporterConstants.OTLP_TYPE:
                        exportedUris.add(buildOTLPExporterURI(kafkaConfig));
                        break;
                    case ExporterConstants.PROMETHEUS_TYPE:
                        exportedUris.add(buildPrometheusExporterURI(kafkaConfig));
                        break;
                    default:
                        LOGGER.error("illegal metrics exporter type: {}", exporterType);
                        break;
                }
            }
        }

        if (kafkaConfig.getBoolean(S3_TELEMETRY_OPS_ENABLED_CONFIG)) {
            exportedUris.add(buildOpsExporterURI());
        }

        return String.join(",", exportedUris);
    }

    private static String buildOTLPExporterURI(KafkaConfig kafkaConfig) {
        StringBuilder uriBuilder = new StringBuilder()
            .append(ExporterConstants.OTLP_TYPE)
            .append(ExporterConstants.URI_DELIMITER)
            .append(ExporterConstants.ENDPOINT).append("=").append(kafkaConfig.getString(S3_TELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG))
            .append("&")
            .append(ExporterConstants.PROTOCOL).append("=").append(kafkaConfig.getString(S3_TELEMETRY_EXPORTER_OTLP_PROTOCOL_CONFIG));
        if (kafkaConfig.getBoolean(S3_TELEMETRY_EXPORTER_OTLP_COMPRESSION_ENABLE_CONFIG)) {
            uriBuilder.append("&").append(ExporterConstants.COMPRESSION).append("=").append("gzip");
        }
        return uriBuilder.toString();
    }

    private static String buildPrometheusExporterURI(KafkaConfig kafkaConfig) {
        return ExporterConstants.PROMETHEUS_TYPE + ExporterConstants.URI_DELIMITER +
            ExporterConstants.HOST + "=" + kafkaConfig.getString(S3_METRICS_EXPORTER_PROM_HOST_CONFIG) + "&" +
            ExporterConstants.PORT + "=" + kafkaConfig.getInt(S3_METRICS_EXPORTER_PROM_PORT_CONFIG);
    }

    private static String buildOpsExporterURI() {
        return ExporterConstants.OPS_TYPE + ExporterConstants.URI_DELIMITER;
    }
}
