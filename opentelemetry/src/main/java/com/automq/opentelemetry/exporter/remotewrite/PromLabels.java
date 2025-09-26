package com.automq.opentelemetry.exporter.remotewrite;

import com.automq.opentelemetry.telemetry.RemoteWrite;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.semconv.ResourceAttributes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class PromLabels {
    // sorted by key in alphabetical order
    private final Map<String, String> kvPairs;
    private int hashcode;

    private PromLabels(Map<String, String> kvPairs) {
        this.kvPairs = kvPairs;
    }

    public static PromLabels fromOTLPMetric(String name, MetricData metricData, Attributes attr) {
        return fromOTLPMetric(name, metricData, attr, Collections.emptyMap());
    }

    public static PromLabels fromOTLPMetric(String name, MetricData metricData, Attributes attr, Map<String, String> extra) {
        Map<String, String> labels = new TreeMap<>();
        labels.put(PromConsts.NAME_LABEL, name);
        metricData.getResource().getAttributes().forEach((k, v) -> {
            if (k.equals(ResourceAttributes.SERVICE_NAME)) {
                labels.put(PromConsts.PROM_JOB_LABEL, v.toString());
            } else if (k.equals(ResourceAttributes.SERVICE_INSTANCE_ID)) {
                labels.put(PromConsts.PROM_INSTANCE_LABEL, v.toString());
            } else {
                labels.put(PromUtils.normalizeLabel(k.toString()), v.toString());
            }
        });
        attr.forEach((k, v) -> labels.put(PromUtils.normalizeLabel(k.getKey()), v.toString()));
        extra.forEach((k, v) -> labels.put(PromUtils.normalizeLabel(k), v));
        return new PromLabels(labels);
    }

    public List<RemoteWrite.Label> toLabels() {
        List<RemoteWrite.Label> labels = new ArrayList<>();
        kvPairs.forEach((k, v) -> labels.add(RemoteWrite.Label.newBuilder().setName(k).setValue(v).build()));
        return labels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PromLabels other = (PromLabels) o;
        return Objects.equals(kvPairs, other.kvPairs);
    }

    @Override
    public int hashCode() {
        int result = this.hashcode;
        if (result == 0) {
            result = 1;
            result = result * 1000003;
            result ^= kvPairs.hashCode();
            this.hashcode = result;
        }

        return result;
    }
}