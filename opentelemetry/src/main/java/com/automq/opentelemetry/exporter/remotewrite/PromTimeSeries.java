package com.automq.opentelemetry.exporter.remotewrite;


import com.automq.opentelemetry.telemetry.RemoteWrite;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PromTimeSeries {
    private final List<RemoteWrite.Label> labels;
    private final List<RemoteWrite.Sample> samples;

    public PromTimeSeries(List<RemoteWrite.Label> labels) {
        this.labels = labels;
        this.samples = new ArrayList<>();
    }

    public void addSample(RemoteWrite.Sample sample) {
        samples.add(sample);
    }

    public RemoteWrite.TimeSeries build() {
        samples.sort(Comparator.comparingLong(RemoteWrite.Sample::getTimestamp));
        return RemoteWrite.TimeSeries.newBuilder()
            .addAllLabels(labels)
            .addAllSamples(samples)
            .build();
    }
}
