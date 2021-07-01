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

package org.apache.kafka.image;

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;


/**
 * The broker metadata image.
 *
 * This class is thread-safe.
 */
public final class MetadataImage {
    public final static MetadataImage EMPTY = new MetadataImage(
        FeaturesImage.EMPTY,
        ClusterImage.EMPTY,
        TopicsImage.EMPTY,
        ConfigurationsImage.EMPTY,
        ClientQuotasImage.EMPTY);

    private final FeaturesImage features;

    private final ClusterImage cluster;

    private final TopicsImage topics;

    private final ConfigurationsImage configs;

    private final ClientQuotasImage clientQuotas;

    public MetadataImage(FeaturesImage features,
                         ClusterImage cluster,
                         TopicsImage topics,
                         ConfigurationsImage configs,
                         ClientQuotasImage clientQuotas) {
        this.features = features;
        this.cluster = cluster;
        this.topics = topics;
        this.configs = configs;
        this.clientQuotas = clientQuotas;
    }

    public boolean isEmpty() {
        return features.isEmpty() &&
            cluster.isEmpty() &&
            topics.isEmpty() &&
            configs.isEmpty() &&
            clientQuotas.isEmpty();
    }

    public FeaturesImage features() {
        return features;
    }

    public ClusterImage cluster() {
        return cluster;
    }

    public TopicsImage topics() {
        return topics;
    }

    public ConfigurationsImage configs() {
        return configs;
    }

    public ClientQuotasImage clientQuotas() {
        return clientQuotas;
    }

    public void write(Consumer<List<ApiMessageAndVersion>> out) {
        features.write(out);
        cluster.write(out);
        topics.write(out);
        configs.write(out);
        clientQuotas.write(out);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetadataImage)) return false;
        MetadataImage other = (MetadataImage) o;
        return features.equals(other.features) &&
            cluster.equals(other.cluster) &&
            topics.equals(other.topics) &&
            configs.equals(other.configs) &&
            clientQuotas.equals(other.clientQuotas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(features, cluster, topics, configs, clientQuotas);
    }

    @Override
    public String toString() {
        return "MetadataImage(features=" + features +
            ", cluster=" + cluster +
            ", topics=" + topics +
            ", configs=" + configs +
            ", clientQuotas=" + clientQuotas +
            ")";
    }
}
