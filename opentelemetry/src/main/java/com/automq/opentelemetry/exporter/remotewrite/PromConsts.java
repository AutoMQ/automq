package com.automq.opentelemetry.exporter.remotewrite;

public class PromConsts {
    public static final String NAME_LABEL = "__name__";
    public static final String PROM_JOB_LABEL = "job";
    public static final String PROM_INSTANCE_LABEL = "instance";
    public static final String METRIC_NAME_SUFFIX_SUM = "_sum";
    public static final String METRIC_NAME_SUFFIX_COUNT = "_count";
    public static final String METRIC_NAME_SUFFIX_BUCKET = "_bucket";
    public static final String LABEL_NAME_LE = "le";
    public static final String LABEL_NAME_QUANTILE = "quantile";
    public static final String LABEL_VALUE_INF = "+Inf";
    public static final String AWS_PROMETHEUS_SERVICE_NAME = "aps";
}
