package org.apache.kafka.connect.automq.metrics;

import org.apache.kafka.connect.automq.runtime.LeaderNodeSelector;
import org.apache.kafka.connect.automq.runtime.RuntimeLeaderSelectorProvider;

import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class ConnectMetricsExportConfig implements MetricsExportConfig {
    
    private final BucketURI metricsBucket;
    private final String clusterId;
    private final int nodeId;
    private final int intervalMs;
    private final List<Pair<String, String>> baseLabels;
    private ObjectStorage objectStorage;
    private LeaderNodeSelector leaderNodeSelector;


    public ConnectMetricsExportConfig(String clusterId, int nodeId, BucketURI metricsBucket, List<Pair<String, String>> baseLabels, int intervalMs) {
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.metricsBucket = metricsBucket;
        this.baseLabels = baseLabels;
        this.intervalMs = intervalMs;
    }
    
    @Override
    public String clusterId() {
        return this.clusterId;
    }

    @Override
    public boolean isLeader() {
        LeaderNodeSelector selector = leaderSelector();
        return selector != null && selector.isLeader();
    }

    public LeaderNodeSelector leaderSelector() {
        if (leaderNodeSelector == null) {
            this.leaderNodeSelector = new RuntimeLeaderSelectorProvider().createSelector();
        }
        return leaderNodeSelector;
    }

    @Override
    public int nodeId() {
        return this.nodeId;
    }

    @Override
    public ObjectStorage objectStorage() {
        if (metricsBucket == null) {
            return null;
        }
        if (this.objectStorage == null) {
            this.objectStorage = ObjectStorageFactory.instance().builder(metricsBucket).threadPrefix("s3-metric").build();
        }
        return this.objectStorage;
    }

    @Override
    public List<Pair<String, String>> baseLabels() {
        return this.baseLabels;
    }

    @Override
    public int intervalMs() {
        return this.intervalMs;
    }
}
