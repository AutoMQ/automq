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
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveDelegationTokenRecord;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.metadata.UpdateNextNodeIdRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Optional;


/**
 * A change to the broker metadata image.
 */
@SuppressWarnings("checkstyle:classFanOutComplexity")
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

    private ScramDelta scramDelta = null;

    private DelegationTokenDelta delegationTokenDelta = null;

    // AutoMQ for Kafka inject start
    private S3StreamsMetadataDelta s3StreamsMetadataDelta = null;

    private S3ObjectsDelta s3ObjectsDelta = null;

    private KVDelta kvDelta = null;
    // AutoMQ for Kafka inject end

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

    public ScramDelta scramDelta() {
        return scramDelta;
    }

    public ScramDelta getOrCreateScramDelta() {
        if (scramDelta == null) scramDelta = new ScramDelta(image.scram());
        return scramDelta;
    }

    public DelegationTokenDelta delegationTokenDelta() {
        return delegationTokenDelta;
    }

    public DelegationTokenDelta getOrCreateDelegationTokenDelta() {
        if (delegationTokenDelta == null) delegationTokenDelta = new DelegationTokenDelta(image.delegationTokens());
        return delegationTokenDelta;
    }

    public Optional<MetadataVersion> metadataVersionChanged() {
        if (featuresDelta == null) {
            return Optional.empty();
        } else {
            return featuresDelta.metadataVersionChange();
        }
    }

    // AutoMQ for Kafka inject start
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

    public KVDelta kvDelta() {
        return kvDelta;
    }

    public KVDelta getOrCreateKVDelta() {
        if (kvDelta == null) {
            kvDelta = new KVDelta(image.kv());
        }
        return kvDelta;
    }
    // AutoMQ for Kafka inject end

    @SuppressWarnings("checkstyle:javaNCSS")
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
            case DELEGATION_TOKEN_RECORD:
                replay((DelegationTokenRecord) record);
                break;
            case USER_SCRAM_CREDENTIAL_RECORD:
                replay((UserScramCredentialRecord) record);
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
            case REMOVE_USER_SCRAM_CREDENTIAL_RECORD:
                replay((RemoveUserScramCredentialRecord) record);
                break;
            case REMOVE_DELEGATION_TOKEN_RECORD:
                replay((RemoveDelegationTokenRecord) record);
                break;
            case NO_OP_RECORD:
                /* NoOpRecord is an empty record and doesn't need to be replayed beyond
                 * updating the highest offset and epoch.
                 */
                break;
            case ZK_MIGRATION_STATE_RECORD:
                replay((ZkMigrationStateRecord) record);
                break;
            case REGISTER_CONTROLLER_RECORD:
                replay((RegisterControllerRecord) record);
                break;
            // AutoMQ for Kafka inject start
            case UPDATE_NEXT_NODE_ID_RECORD:
                replay((UpdateNextNodeIdRecord) record);
                break;
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
            case S3_STREAM_SET_OBJECT_RECORD:
                replay((S3StreamSetObjectRecord) record);
                break;
            case REMOVE_STREAM_SET_OBJECT_RECORD:
                replay((RemoveStreamSetObjectRecord) record);
                break;
            case S3_OBJECT_RECORD:
                replay((S3ObjectRecord) record);
                break;
            case REMOVE_S3_OBJECT_RECORD:
                replay((RemoveS3ObjectRecord) record);
                break;
            case ASSIGNED_S3_OBJECT_ID_RECORD:
                replay((AssignedS3ObjectIdRecord) record);
                break;
            case ASSIGNED_STREAM_ID_RECORD:
                replay((AssignedStreamIdRecord) record);
                break;
            case NODE_WALMETADATA_RECORD:
                replay((NodeWALMetadataRecord) record);
                break;
            case REMOVE_NODE_WALMETADATA_RECORD:
                replay((RemoveNodeWALMetadataRecord) record);
                break;
            case KVRECORD:
                replay((KVRecord) record);
                break;
            case REMOVE_KVRECORD:
                replay((RemoveKVRecord) record);
                break;
            case S3_STREAM_END_OFFSETS_RECORD:
                replay((S3StreamEndOffsetsRecord) record);
                break;
                // AutoMQ for Kafka inject end
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

    public void replay(DelegationTokenRecord record) {
        getOrCreateDelegationTokenDelta().replay(record);
    }

    public void replay(RemoveDelegationTokenRecord record) {
        getOrCreateDelegationTokenDelta().replay(record);
    }

    public void replay(UserScramCredentialRecord record) {
        getOrCreateScramDelta().replay(record);
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
            getOrCreateScramDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateDelegationTokenDelta().handleMetadataVersionChange(changedMetadataVersion);
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

    public void replay(RemoveUserScramCredentialRecord record) {
        getOrCreateScramDelta().replay(record);
    }

    public void replay(ZkMigrationStateRecord record) {
        getOrCreateFeaturesDelta().replay(record);
    }

    public void replay(RegisterControllerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    // AutoMQ for Kafka inject start

    public void replay(UpdateNextNodeIdRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

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

    public void replay(S3StreamSetObjectRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RemoveStreamSetObjectRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(S3ObjectRecord record) {
        getOrCreateObjectsMetadataDelta().replay(record);
    }

    public void replay(RemoveS3ObjectRecord record) {
        getOrCreateObjectsMetadataDelta().replay(record);
    }

    public void replay(AssignedS3ObjectIdRecord record) {
        getOrCreateObjectsMetadataDelta().replay(record);
    }

    public void replay(AssignedStreamIdRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(NodeWALMetadataRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(RemoveNodeWALMetadataRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }

    public void replay(KVRecord record) {
        getOrCreateKVDelta().replay(record);
    }

    public void replay(RemoveKVRecord record) {
        getOrCreateKVDelta().replay(record);
    }

    public void replay(S3StreamEndOffsetsRecord record) {
        getOrCreateStreamsMetadataDelta().replay(record);
    }
    // AutoMQ for Kafka inject end

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
        getOrCreateScramDelta().finishSnapshot();
        getOrCreateDelegationTokenDelta().finishSnapshot();
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

        // AutoMQ for Kafka inject start
        S3StreamsMetadataImage newStreamMetadata = getNewS3StreamsMetadataImage();
        S3ObjectsImage newS3ObjectsMetadata = getNewS3ObjectsMetadataImage();
        KVImage newKVImage = getNewKVImage();
        // AutoMQ for Kafka inject end

        ScramImage newScram;
        if (scramDelta == null) {
            newScram = image.scram();
        } else {
            newScram = scramDelta.apply();
        }
        DelegationTokenImage newDelegationTokens;
        if (delegationTokenDelta == null) {
            newDelegationTokens = image.delegationTokens();
        } else {
            newDelegationTokens = delegationTokenDelta.apply();
        }
        return new MetadataImage(
            provenance,
            newFeatures,
            newCluster,
            newTopics,
            newConfigs,
            newClientQuotas,
            newProducerIds,
            newAcls,
            newScram,
            newDelegationTokens,
            newStreamMetadata,
            newS3ObjectsMetadata,
            newKVImage
        );
    }

    // AutoMQ for Kafka inject start

    private S3StreamsMetadataImage getNewS3StreamsMetadataImage() {
        return s3StreamsMetadataDelta == null ?
            image.streamsMetadata() : s3StreamsMetadataDelta.apply();
    }

    private S3ObjectsImage getNewS3ObjectsMetadataImage() {
        return s3ObjectsDelta == null ?
            image.objectsMetadata() : s3ObjectsDelta.apply();
    }

    private KVImage getNewKVImage() {
        return kvDelta == null ?
            image.kv() : kvDelta.apply();
    }
    // AutoMQ for Kafka inject end

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
            ", scramDelta=" + scramDelta +
            ", delegationTokenDelta=" + delegationTokenDelta +
            ", streamMetadataDelta=" + s3StreamsMetadataDelta +
            ", objectsMetadataDelta=" + s3ObjectsDelta +
            ", kvDelta=" + kvDelta +
            ')';
    }
}
