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

import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Optional;


/**
 * A change to the broker metadata image.
 */
public final class MetadataDelta {
    public static class Builder {
        private MetadataImage image = MetadataImage.EMPTY;

        public Builder setImage(MetadataImage image) {
            this.image = image;
            return this;
        }

        public MetadataDelta build() {
            return new MetadataDelta(image);
        }
    }

    private final MetadataImage image;

    private FeaturesDelta featuresDelta = null;

    private ClusterDelta clusterDelta = null;

    private TopicsDelta topicsDelta = null;

    private ConfigurationsDelta configsDelta = null;

    private ClientQuotasDelta clientQuotasDelta = null;

    private ProducerIdsDelta producerIdsDelta = null;

    private AclsDelta aclsDelta = null;

    // Kafka on S3 inject start
    private S3StreamsMetadataDelta s3StreamsMetadataDelta = null;

    private S3ObjectsDelta s3ObjectsDelta = null;

    // Kafka on S3 inject end

    public MetadataDelta(MetadataImage image) {
        this.image = image;
    }

    public MetadataImage image() {
        return image;
    }

    public FeaturesDelta featuresDelta() {
        return featuresDelta;
    }

    public FeaturesDelta getOrCreateFeaturesDelta() {
        if (featuresDelta == null) featuresDelta = new FeaturesDelta(image.features());
        return featuresDelta;
    }

    public ClusterDelta clusterDelta() {
        return clusterDelta;
    }

    public ClusterDelta getOrCreateClusterDelta() {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        return clusterDelta;
    }

    public TopicsDelta topicsDelta() {
        return topicsDelta;
    }

    public TopicsDelta getOrCreateTopicsDelta() {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        return topicsDelta;
    }

    public ConfigurationsDelta configsDelta() {
        return configsDelta;
    }

    public ConfigurationsDelta getOrCreateConfigsDelta() {
        if (configsDelta == null) configsDelta = new ConfigurationsDelta(image.configs());
        return configsDelta;
    }

    public ClientQuotasDelta clientQuotasDelta() {
        return clientQuotasDelta;
    }

    public ClientQuotasDelta getOrCreateClientQuotasDelta() {
        if (clientQuotasDelta == null) clientQuotasDelta = new ClientQuotasDelta(image.clientQuotas());
        return clientQuotasDelta;
    }

    public ProducerIdsDelta producerIdsDelta() {
        return producerIdsDelta;
    }

    public ProducerIdsDelta getOrCreateProducerIdsDelta() {
        if (producerIdsDelta == null) {
            producerIdsDelta = new ProducerIdsDelta(image.producerIds());
        }
        return producerIdsDelta;
    }

    public AclsDelta aclsDelta() {
        return aclsDelta;
    }

    public AclsDelta getOrCreateAclsDelta() {
        if (aclsDelta == null) aclsDelta = new AclsDelta(image.acls());
        return aclsDelta;
    }


    // Kafka on S3 inject start
    public S3StreamsMetadataDelta streamMetadataDelta() {
        return s3StreamsMetadataDelta;
    }

    public S3StreamsMetadataDelta getOrCreateStreamsMetadataDelta() {
        if (s3StreamsMetadataDelta == null) {
            s3StreamsMetadataDelta = new S3StreamsMetadataDelta(image.streamsMetadata());
        }
        return s3StreamsMetadataDelta;
    }

    public S3ObjectsDelta objectsMetadataDelta() {
        return s3ObjectsDelta;
    }

    public S3ObjectsDelta getOrCreateObjectsMetadataDelta() {
        if (s3ObjectsDelta == null) {
            s3ObjectsDelta = new S3ObjectsDelta(image.objectsMetadata());
        }
        return s3ObjectsDelta;
    }

    // Kafka on S3 inject end

    public Optional<MetadataVersion> metadataVersionChanged() {
        if (featuresDelta == null) {
            return Optional.empty();
        } else {
            return featuresDelta.metadataVersionChange();
        }
    }

    public void replay(ApiMessage record) {
        MetadataRecordType type = MetadataRecordType.fromId(record.apiKey());
        switch (type) {
            case REGISTER_BROKER_RECORD:
                replay((RegisterBrokerRecord) record);
                break;
            case UNREGISTER_BROKER_RECORD:
                replay((UnregisterBrokerRecord) record);
                break;
            case TOPIC_RECORD:
                replay((TopicRecord) record);
                break;
            case PARTITION_RECORD:
                replay((PartitionRecord) record);
                break;
            case CONFIG_RECORD:
                replay((ConfigRecord) record);
                break;
            case PARTITION_CHANGE_RECORD:
                replay((PartitionChangeRecord) record);
                break;
            case FENCE_BROKER_RECORD:
                replay((FenceBrokerRecord) record);
                break;
            case UNFENCE_BROKER_RECORD:
                replay((UnfenceBrokerRecord) record);
                break;
            case REMOVE_TOPIC_RECORD:
                replay((RemoveTopicRecord) record);
                break;
            case FEATURE_LEVEL_RECORD:
                replay((FeatureLevelRecord) record);
                break;
            case CLIENT_QUOTA_RECORD:
                replay((ClientQuotaRecord) record);
                break;
            case PRODUCER_IDS_RECORD:
                replay((ProducerIdsRecord) record);
                break;
            case BROKER_REGISTRATION_CHANGE_RECORD:
                replay((BrokerRegistrationChangeRecord) record);
                break;
            case ACCESS_CONTROL_ENTRY_RECORD:
                replay((AccessControlEntryRecord) record);
                break;
            case REMOVE_ACCESS_CONTROL_ENTRY_RECORD:
                replay((RemoveAccessControlEntryRecord) record);
                break;
            case NO_OP_RECORD:
                /* NoOpRecord is an empty record and doesn't need to be replayed beyond
                 * updating the highest offset and epoch.
                 */
                break;
            case ZK_MIGRATION_STATE_RECORD:
                // TODO handle this
                break;
            // Kafka on S3 inject start
            case S3_STREAM_RECORD:
                replay((S3StreamRecord) record);
                break;
            case REMOVE_S3_STREAM_RECORD:
                replay((RemoveS3StreamRecord) record);
                break;
            case RANGE_RECORD:
                replay((RangeRecord) record);
                break;
            case REMOVE_RANGE_RECORD:
                replay((RemoveRangeRecord) record);
                break;
            case S3_STREAM_OBJECT_RECORD:
                replay((S3StreamObjectRecord) record);
                break;
            case REMOVE_S3_STREAM_OBJECT_RECORD:
                replay((RemoveS3StreamObjectRecord) record);
                break;
            case WALOBJECT_RECORD:
                replay((WALObjectRecord) record);
                break;
            case REMOVE_WALOBJECT_RECORD:
                replay((RemoveWALObjectRecord) record);
                break;
            case S3_OBJECT_RECORD:
                replay((S3ObjectRecord) record);
                break;
            case REMOVE_S3_OBJECT_RECORD:
                replay((RemoveS3ObjectRecord) record);
                break;
            // Kafka on S3 inject end
            default:
                throw new RuntimeException("Unknown metadata record type " + type);
        }
    }

    public void replay(RegisterBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(UnregisterBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(TopicRecord record) {
        getOrCreateTopicsDelta().replay(record);
    }

    public void replay(PartitionRecord record) {
        getOrCreateTopicsDelta().replay(record);
    }

    public void replay(ConfigRecord record) {
        getOrCreateConfigsDelta().replay(record);
    }

    public void replay(PartitionChangeRecord record) {
        getOrCreateTopicsDelta().replay(record);
    }

    public void replay(FenceBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(UnfenceBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(RemoveTopicRecord record) {
        String topicName = getOrCreateTopicsDelta().replay(record);
        getOrCreateConfigsDelta().replay(record, topicName);
    }

    public void replay(FeatureLevelRecord record) {
        getOrCreateFeaturesDelta().replay(record);
        featuresDelta.metadataVersionChange().ifPresent(changedMetadataVersion -> {
            // If any feature flags change, need to immediately check if any metadata needs to be downgraded.
            getOrCreateClusterDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateConfigsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateTopicsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateClientQuotasDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateProducerIdsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateAclsDelta().handleMetadataVersionChange(changedMetadataVersion);
        });
    }

    public void replay(BrokerRegistrationChangeRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(ClientQuotaRecord record) {
        getOrCreateClientQuotasDelta().replay(record);
    }

    public void replay(ProducerIdsRecord record) {
        getOrCreateProducerIdsDelta().replay(record);
    }

    public void replay(AccessControlEntryRecord record) {
        getOrCreateAclsDelta().replay(record);
    }

    public void replay(RemoveAccessControlEntryRecord record) {
        getOrCreateAclsDelta().replay(record);
    }

    // Kafka on S3 inject start

    public void replay(S3StreamRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RemoveS3StreamRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RangeRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RemoveRangeRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(S3StreamObjectRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(WALObjectRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RemoveWALObjectRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(S3ObjectRecord record) {
        getOrCreateObjectsMetadataDelta().replay(record);
    }

    public void replay(RemoveS3ObjectRecord record) {
        getOrCreateObjectsMetadataDelta().replay(record);
    }

    // Kafka on S3 inject end

    /**
     * Create removal deltas for anything which was in the base image, but which was not
     * referenced in the snapshot records we just applied.
     */
    public void finishSnapshot() {
        getOrCreateFeaturesDelta().finishSnapshot();
        getOrCreateClusterDelta().finishSnapshot();
        getOrCreateTopicsDelta().finishSnapshot();
        getOrCreateConfigsDelta().finishSnapshot();
        getOrCreateClientQuotasDelta().finishSnapshot();
        getOrCreateProducerIdsDelta().finishSnapshot();
        getOrCreateAclsDelta().finishSnapshot();
    }

    public MetadataImage apply(MetadataProvenance provenance) {
        FeaturesImage newFeatures;
        if (featuresDelta == null) {
            newFeatures = image.features();
        } else {
            newFeatures = featuresDelta.apply();
        }
        ClusterImage newCluster;
        if (clusterDelta == null) {
            newCluster = image.cluster();
        } else {
            newCluster = clusterDelta.apply();
        }
        TopicsImage newTopics;
        if (topicsDelta == null) {
            newTopics = image.topics();
        } else {
            newTopics = topicsDelta.apply();
        }
        ConfigurationsImage newConfigs;
        if (configsDelta == null) {
            newConfigs = image.configs();
        } else {
            newConfigs = configsDelta.apply();
        }
        ClientQuotasImage newClientQuotas;
        if (clientQuotasDelta == null) {
            newClientQuotas = image.clientQuotas();
        } else {
            newClientQuotas = clientQuotasDelta.apply();
        }
        ProducerIdsImage newProducerIds;
        if (producerIdsDelta == null) {
            newProducerIds = image.producerIds();
        } else {
            newProducerIds = producerIdsDelta.apply();
        }
        AclsImage newAcls;
        if (aclsDelta == null) {
            newAcls = image.acls();
        } else {
            newAcls = aclsDelta.apply();
        }

        // Kafka on S3 inject start
        S3StreamsMetadataImage newStreamMetadata;
        if (s3StreamsMetadataDelta == null) {
            newStreamMetadata = image.streamsMetadata();
        } else {
            newStreamMetadata = s3StreamsMetadataDelta.apply();
        }
        S3ObjectsImage newS3ObjectsMetadata;
        if (s3ObjectsDelta == null) {
            newS3ObjectsMetadata = image.objectsMetadata();
        } else {
            newS3ObjectsMetadata = s3ObjectsDelta.apply();
        }
        // Kafka on S3 inject end
        return new MetadataImage(
            provenance,
            newFeatures,
            newCluster,
            newTopics,
            newConfigs,
            newClientQuotas,
            newProducerIds,
            newAcls,
            newStreamMetadata,
            newS3ObjectsMetadata
        );
    }

    @Override
    public String toString() {
        return "MetadataDelta(" +
            "featuresDelta=" + featuresDelta +
            ", clusterDelta=" + clusterDelta +
            ", topicsDelta=" + topicsDelta +
            ", configsDelta=" + configsDelta +
            ", clientQuotasDelta=" + clientQuotasDelta +
            ", producerIdsDelta=" + producerIdsDelta +
            ", aclsDelta=" + aclsDelta +
            ", streamMetadataDelta=" + s3StreamsMetadataDelta +
            ", objectsMetadataDelta=" + s3ObjectsDelta +
            ')';
    }
}
