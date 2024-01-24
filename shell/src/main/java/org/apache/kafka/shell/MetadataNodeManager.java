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

package org.apache.kafka.shell;

import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.AccessControlEntryRecordJsonConverter;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecordJsonConverter;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecordJsonConverter;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.PartitionRecordJsonConverter;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RangeRecordJsonConverter;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecordJsonConverter;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecordJsonConverter;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamRecordJsonConverter;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.metadata.UpdateNextNodeIdRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.shell.MetadataNode.DirectoryNode;
import org.apache.kafka.shell.MetadataNode.FileNode;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.kafka.metadata.LeaderRecoveryState.NO_CHANGE;

/**
 * Maintains the in-memory metadata for the metadata tool.
 */
public final class MetadataNodeManager implements AutoCloseable {
    private static final int NO_LEADER_CHANGE = -2;

    private static final Logger log = LoggerFactory.getLogger(MetadataNodeManager.class);

    public static class Data {
        private final DirectoryNode root = new DirectoryNode();
        private String workingDirectory = "/";

        public DirectoryNode root() {
            return root;
        }

        public String workingDirectory() {
            return workingDirectory;
        }

        public void setWorkingDirectory(String workingDirectory) {
            this.workingDirectory = workingDirectory;
        }
    }

    class LogListener implements RaftClient.Listener<ApiMessageAndVersion> {
        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    log.debug("handleCommits " + batch.records() + " at offset " + batch.lastOffset());
                    DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                    dir.create("offset").setContents(String.valueOf(batch.lastOffset()));
                    for (ApiMessageAndVersion messageAndVersion : batch.records()) {
                        handleMessage(messageAndVersion.message());
                    }
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    for (ApiMessageAndVersion messageAndVersion : batch) {
                        handleMessage(messageAndVersion.message());
                    }
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leader) {
            appendEvent("handleNewLeader", () -> {
                log.debug("handleNewLeader " + leader);
                DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                dir.create("leader").setContents(leader.toString());
            }, null);
        }

        @Override
        public void beginShutdown() {
            log.debug("Metadata log listener sent beginShutdown");
        }
    }

    private final Data data = new Data();
    private final LogListener logListener = new LogListener();
    private final ObjectMapper objectMapper;
    private final KafkaEventQueue queue;

    public MetadataNodeManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new Jdk8Module());
        this.queue = new KafkaEventQueue(Time.SYSTEM,
            new LogContext("[node-manager-event-queue] "), "");
    }

    public void setup() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        appendEvent("createShellNodes", () -> {
            DirectoryNode directory = data.root().mkdirs("local");
            directory.create("version").setContents(AppInfoParser.getVersion());
            directory.create("commitId").setContents(AppInfoParser.getCommitId());
            future.complete(null);
        }, future);
        future.get();
    }

    public LogListener logListener() {
        return logListener;
    }

    // VisibleForTesting
    Data getData() {
        return data;
    }

    @Override
    public void close() throws Exception {
        queue.close();
    }

    public void visit(Consumer<Data> consumer) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        appendEvent("visit", () -> {
            consumer.accept(data);
            future.complete(null);
        }, future);
        future.get();
    }

    private void appendEvent(String name, Runnable runnable, CompletableFuture<?> future) {
        queue.append(new EventQueue.Event() {
            @Override
            public void run() throws Exception {
                runnable.run();
            }

            @Override
            public void handleException(Throwable e) {
                log.error("Unexpected error while handling event " + name, e);
                if (future != null) {
                    future.completeExceptionally(e);
                }
            }
        });
    }

    // VisibleForTesting
    void handleMessage(ApiMessage message) {
        try {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            handleCommitImpl(type, message);
        } catch (Exception e) {
            log.error("Error processing record of type " + message.apiKey(), e);
        }
    }

    private void handleCommitImpl(MetadataRecordType type, ApiMessage message)
        throws Exception {
        switch (type) {
            case REGISTER_BROKER_RECORD: {
                DirectoryNode brokersNode = data.root.mkdirs("brokers");
                RegisterBrokerRecord record = (RegisterBrokerRecord) message;
                DirectoryNode brokerNode = brokersNode.
                    mkdirs(Integer.toString(record.brokerId()));
                FileNode registrationNode = brokerNode.create("registration");
                registrationNode.setContents(record.toString());
                brokerNode.create("isFenced").setContents("true");
                break;
            }
            case UNREGISTER_BROKER_RECORD: {
                UnregisterBrokerRecord record = (UnregisterBrokerRecord) message;
                data.root.rmrf("brokers", Integer.toString(record.brokerId()));
                break;
            }
            case TOPIC_RECORD: {
                TopicRecord record = (TopicRecord) message;
                DirectoryNode topicsDirectory = data.root.mkdirs("topics");
                DirectoryNode topicDirectory = topicsDirectory.mkdirs(record.name());
                topicDirectory.create("id").setContents(record.topicId().toString());
                topicDirectory.create("name").setContents(record.name().toString());
                DirectoryNode topicIdsDirectory = data.root.mkdirs("topicIds");
                topicIdsDirectory.addChild(record.topicId().toString(), topicDirectory);
                break;
            }
            case PARTITION_RECORD: {
                PartitionRecord record = (PartitionRecord) message;
                DirectoryNode topicDirectory =
                    data.root.mkdirs("topicIds").mkdirs(record.topicId().toString());
                DirectoryNode partitionDirectory =
                    topicDirectory.mkdirs(Integer.toString(record.partitionId()));
                JsonNode node = PartitionRecordJsonConverter.
                    write(record, PartitionRecord.HIGHEST_SUPPORTED_VERSION);
                partitionDirectory.create("data").setContents(node.toPrettyString());
                break;
            }
            case CONFIG_RECORD: {
                ConfigRecord record = (ConfigRecord) message;
                String typeString = "";
                switch (ConfigResource.Type.forId(record.resourceType())) {
                    case BROKER:
                        typeString = "broker";
                        break;
                    case TOPIC:
                        typeString = "topic";
                        break;
                    default:
                        throw new RuntimeException("Error processing CONFIG_RECORD: " +
                            "Can't handle ConfigResource.Type " + record.resourceType());
                }
                DirectoryNode configDirectory = data.root.mkdirs("configs").
                    mkdirs(typeString).mkdirs(record.resourceName().isEmpty() ? "<default>" : record.resourceName());
                if (record.value() == null) {
                    configDirectory.rmrf(record.name());
                } else {
                    configDirectory.create(record.name()).setContents(record.value());
                }
                break;
            }
            case PARTITION_CHANGE_RECORD: {
                PartitionChangeRecord record = (PartitionChangeRecord) message;
                FileNode file = data.root.file("topicIds", record.topicId().toString(),
                    Integer.toString(record.partitionId()), "data");
                JsonNode node = objectMapper.readTree(file.contents());
                PartitionRecord partition = PartitionRecordJsonConverter.
                    read(node, PartitionRecord.HIGHEST_SUPPORTED_VERSION);
                if (record.isr() != null) {
                    partition.setIsr(record.isr());
                }
                if (record.leader() != NO_LEADER_CHANGE) {
                    partition.setLeader(record.leader());
                    partition.setLeaderEpoch(partition.leaderEpoch() + 1);
                }
                if (record.leaderRecoveryState() != NO_CHANGE) {
                    partition.setLeaderRecoveryState(record.leaderRecoveryState());
                }
                partition.setPartitionEpoch(partition.partitionEpoch() + 1);
                file.setContents(PartitionRecordJsonConverter.write(partition,
                    PartitionRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                break;
            }
            case FENCE_BROKER_RECORD: {
                FenceBrokerRecord record = (FenceBrokerRecord) message;
                data.root.mkdirs("brokers", Integer.toString(record.id())).
                    create("isFenced").setContents("true");
                break;
            }
            case UNFENCE_BROKER_RECORD: {
                UnfenceBrokerRecord record = (UnfenceBrokerRecord) message;
                data.root.mkdirs("brokers", Integer.toString(record.id())).
                    create("isFenced").setContents("false");
                break;
            }
            case BROKER_REGISTRATION_CHANGE_RECORD: {
                BrokerRegistrationChangeRecord record = (BrokerRegistrationChangeRecord) message;
                BrokerRegistrationFencingChange fencingChange =
                    BrokerRegistrationFencingChange.fromValue(record.fenced()).get();
                if (fencingChange != BrokerRegistrationFencingChange.NONE) {
                    data.root.mkdirs("brokers", Integer.toString(record.brokerId()))
                        .create("isFenced").setContents(Boolean.toString(fencingChange.asBoolean().get()));
                }
                BrokerRegistrationInControlledShutdownChange inControlledShutdownChange =
                    BrokerRegistrationInControlledShutdownChange.fromValue(record.inControlledShutdown()).get();
                if (inControlledShutdownChange != BrokerRegistrationInControlledShutdownChange.NONE) {
                    data.root.mkdirs("brokers", Integer.toString(record.brokerId()))
                        .create("inControlledShutdown").setContents(Boolean.toString(inControlledShutdownChange.asBoolean().get()));
                }
                break;
            }
            case REMOVE_TOPIC_RECORD: {
                RemoveTopicRecord record = (RemoveTopicRecord) message;
                DirectoryNode topicsDirectory =
                    data.root.directory("topicIds", record.topicId().toString());
                String name = topicsDirectory.file("name").contents();
                data.root.rmrf("topics", name);
                data.root.rmrf("topicIds", record.topicId().toString());
                break;
            }
            case CLIENT_QUOTA_RECORD: {
                ClientQuotaRecord record = (ClientQuotaRecord) message;
                List<String> directories = clientQuotaRecordDirectories(record.entity());
                DirectoryNode node = data.root;
                for (String directory : directories) {
                    node = node.mkdirs(directory);
                }
                if (record.remove())
                    node.rmrf(record.key());
                else
                    node.create(record.key()).setContents(record.value() + "");
                break;
            }
            case PRODUCER_IDS_RECORD: {
                ProducerIdsRecord record = (ProducerIdsRecord) message;
                DirectoryNode producerIds = data.root.mkdirs("producerIds");
                producerIds.create("lastBlockBrokerId").setContents(record.brokerId() + "");
                producerIds.create("lastBlockBrokerEpoch").setContents(record.brokerEpoch() + "");

                producerIds.create("nextBlockStartId").setContents(record.nextProducerId() + "");
                break;
            }
            case ACCESS_CONTROL_ENTRY_RECORD: {
                AccessControlEntryRecord record = (AccessControlEntryRecord) message;
                DirectoryNode acls = data.root.mkdirs("acl").mkdirs("id");
                FileNode file = acls.create(record.id().toString());
                file.setContents(AccessControlEntryRecordJsonConverter.write(record,
                    AccessControlEntryRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                break;
            }
            case REMOVE_ACCESS_CONTROL_ENTRY_RECORD: {
                RemoveAccessControlEntryRecord record = (RemoveAccessControlEntryRecord) message;
                DirectoryNode acls = data.root.mkdirs("acl").mkdirs("id");
                acls.rmrf(record.id().toString());
                break;
            }
            case FEATURE_LEVEL_RECORD: {
                FeatureLevelRecord record = (FeatureLevelRecord) message;
                DirectoryNode features = data.root.mkdirs("features");
                if (record.featureLevel() == 0) {
                    features.rmrf(record.name());
                } else {
                    FileNode file = features.create(record.name());
                    file.setContents(FeatureLevelRecordJsonConverter.write(record,
                        FeatureLevelRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                }
                break;
            }
            case NO_OP_RECORD: {
                break;
            }
            default:
                handleExtCommitImpl(type, message);
        }
    }

    static List<String> clientQuotaRecordDirectories(List<EntityData> entityData) {
        List<String> result = new ArrayList<>();
        result.add("client-quotas");
        TreeMap<String, EntityData> entries = new TreeMap<>();
        entityData.forEach(e -> entries.put(e.entityType(), e));
        for (Map.Entry<String, EntityData> entry : entries.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue().entityName() == null ?
                "<default>" : entry.getValue().entityName());
        }
        return result;
    }

    /**
     * <h3>File Hierarchy for AutoMQ Custom Metadata</h3>
     * <pre>
     * .
     * ├── kvRecords
     * │   └── _kafka_FVWxwMClSYObxlUORZANzA
     * │       └── 4IHvQLhZSJ6szHPcyady3Q
     * │           └── 0
     * ├── nodes
     * │   └── 1 - node id
     * │       ├── s3StreamSetObjects
     * │       │   └── 4 - object id
     * │       │       ├── dataTimeInMs
     * │       │       ├── orderId
     * │       │       └── ranges
     * │       └── walMetadata
     * ├── s3Objects
     * │   └── 1 - object id
     * └── s3Streams
     *     └── 0 - stream id
     *         ├── ranges
     *         │   └── 0 - range index
     *         └── s3StreamObjects
     *             └── 3 - object id
     * </pre>
     */
    private void handleExtCommitImpl(MetadataRecordType type, ApiMessage message) {
        switch (type) {
            // For AutoMQ-related records.
            case S3_STREAM_RECORD: {
                S3StreamRecord record = (S3StreamRecord) message;
                DirectoryNode s3Streams = data.root.mkdirs("s3Streams");
                DirectoryNode s3Stream = s3Streams.mkdirs(Long.toString(record.streamId()));
                String data = S3StreamRecordJsonConverter.write(record, S3StreamRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString();
                s3Stream.create("data").setContents(data);
                break;
            }
            case REMOVE_S3_STREAM_RECORD: {
                RemoveS3StreamRecord record = (RemoveS3StreamRecord) message;
                data.root.rmrf("s3Streams", Long.toString(record.streamId()));
                break;
            }
            case RANGE_RECORD: {
                RangeRecord record = (RangeRecord) message;
                DirectoryNode streams = data.root.mkdirs("s3Streams");
                DirectoryNode stream = streams.mkdirs(Long.toString(record.streamId()));
                DirectoryNode ranges = stream.mkdirs("ranges");
                FileNode file = ranges.create(Integer.toString(record.rangeIndex()));
                file.setContents(RangeRecordJsonConverter.write(record, RangeRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                break;
            }
            case REMOVE_RANGE_RECORD: {
                RemoveRangeRecord record = (RemoveRangeRecord) message;
                data.root.rmrf("s3Streams", Long.toString(record.streamId()), "ranges", Integer.toString(record.rangeIndex()));
                break;
            }
            case S3_STREAM_OBJECT_RECORD: {
                S3StreamObjectRecord record = (S3StreamObjectRecord) message;
                DirectoryNode s3StreamObjects = data.root.mkdirs("s3Streams", Long.toString(record.streamId()), "s3StreamObjects");
                FileNode file = s3StreamObjects.create(Long.toString(record.objectId()));
                file.setContents(S3StreamObjectRecordJsonConverter.write(record, S3StreamObjectRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                break;
            }
            case REMOVE_S3_STREAM_OBJECT_RECORD: {
                RemoveS3StreamObjectRecord record = (RemoveS3StreamObjectRecord) message;
                data.root.rmrf("s3Streams", Long.toString(record.streamId()), "s3StreamObjects", Long.toString(record.objectId()));
                break;
            }
            case S3_STREAM_SET_OBJECT_RECORD: {
                S3StreamSetObjectRecord record = (S3StreamSetObjectRecord) message;
                DirectoryNode streamSetObject = data.root.mkdirs("nodes", Integer.toString(record.nodeId()), "s3StreamSetObjects", Long.toString(record.objectId()));
                streamSetObject.create("orderId").setContents(Long.toString(record.orderId()));
                streamSetObject.create("dataTimeInMs").setContents(Long.toString(record.dataTimeInMs()));
                byte[] bytes = record.ranges();
                // Decode ranges and convert it to human-readable string.
                List<StreamOffsetRange> ranges = S3StreamSetObject.decode(bytes);
                streamSetObject.create("ranges").setContents(ranges.toString());
                break;
            }
            case REMOVE_STREAM_SET_OBJECT_RECORD: {
                RemoveStreamSetObjectRecord record = (RemoveStreamSetObjectRecord) message;
                data.root.rmrf("nodes", Integer.toString(record.nodeId()), "s3StreamSetObjects", Long.toString(record.objectId()));
                break;
            }
            case S3_OBJECT_RECORD: {
                S3ObjectRecord record = (S3ObjectRecord) message;
                DirectoryNode s3Objects = data.root.mkdirs("s3Objects");
                FileNode file = s3Objects.create(Long.toString(record.objectId()));
                file.setContents(S3ObjectRecordJsonConverter.write(record, S3ObjectRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                break;
            }
            case REMOVE_S3_OBJECT_RECORD: {
                RemoveS3ObjectRecord record = (RemoveS3ObjectRecord) message;
                data.root.rmrf("s3Objects", Long.toString(record.objectId()));
                break;
            }
            case ASSIGNED_STREAM_ID_RECORD: {
                AssignedStreamIdRecord record = (AssignedStreamIdRecord) message;
                DirectoryNode streamIdsNode = data.root.mkdirs("s3Streams");
                // Empty directory here. We just need to create the directory.
                streamIdsNode.mkdirs(Long.toString(record.assignedStreamId()));
                break;
            }
            case ASSIGNED_S3_OBJECT_ID_RECORD: {
                AssignedS3ObjectIdRecord record = (AssignedS3ObjectIdRecord) message;
                DirectoryNode s3ObjectsNode = data.root.mkdirs("s3Objects");
                s3ObjectsNode.create(Long.toString(record.assignedS3ObjectId()));
                break;
            }
            case NODE_WALMETADATA_RECORD: {
                NodeWALMetadataRecord record = (NodeWALMetadataRecord) message;
                DirectoryNode nodeId = data.root.mkdirs("nodes", Integer.toString(record.nodeId()));
                FileNode file = nodeId.create("walMetadata");
                file.setContents(NodeWALMetadataRecordJsonConverter.write(record, NodeWALMetadataRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString());
                break;
            }
            case REMOVE_NODE_WALMETADATA_RECORD: {
                RemoveNodeWALMetadataRecord record = (RemoveNodeWALMetadataRecord) message;
                data.root.rmrf("nodes", Integer.toString(record.nodeId()), "walMetadata");
                break;
            }
            case KVRECORD: {
                KVRecord record = (KVRecord) message;
                DirectoryNode kvRecords = data.root.mkdirs("kvRecords");
                for (KVRecord.KeyValue kv : record.keyValues()) {
                    DirectoryNode node = kvRecords;
                    String fileName = kv.key();
                    String[] filePathArray = kv.key().split("/");
                    if (filePathArray.length > 1) {
                        String[] dirPathArray = Arrays.copyOfRange(filePathArray, 0, filePathArray.length - 1);
                        node = node.mkdirs(dirPathArray);
                        fileName = filePathArray[filePathArray.length - 1];
                    }
                    node.create(fileName).setContents(Arrays.toString(kv.value()));
                }
                break;
            }
            case REMOVE_KVRECORD: {
                RemoveKVRecord record = (RemoveKVRecord) message;
                for (String key : record.keys()) {
                    String[] pathArray = Stream.concat(Stream.of("kvRecords"), Stream.of(key.split("/"))).toArray(String[]::new);
                    data.root.rmrf(pathArray);
                }
                break;
            }
            case UPDATE_NEXT_NODE_ID_RECORD: {
                UpdateNextNodeIdRecord record = (UpdateNextNodeIdRecord) message;
                data.root.mkdirs("nodes", Integer.toString(record.nodeId()));
                break;
            }
        }

    }
}
