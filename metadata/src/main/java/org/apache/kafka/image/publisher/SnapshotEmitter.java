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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RaftSnapshotWriter;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.slf4j.Logger;

import java.util.Optional;


public class SnapshotEmitter implements SnapshotGenerator.Emitter {
    /**
     * The maximum number of records we will put in each snapshot batch by default.
     *
     * From the perspective of the Raft layer, the limit on batch size is specified in terms of
     * bytes, not number of records. See MAX_BATCH_SIZE_BYTES in KafkaRaftClient for details.
     * However, it's more convenient to limit the batch size here in terms of number of records.
     * So we chose a low number that will not cause problems.
     */
    private final static int DEFAULT_BATCH_SIZE = 1024;

    public static class Builder {
        private int nodeId = 0;
        private RaftClient<ApiMessageAndVersion> raftClient = null;
        private int batchSize = DEFAULT_BATCH_SIZE;

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setRaftClient(RaftClient<ApiMessageAndVersion> raftClient) {
            this.raftClient = raftClient;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public SnapshotEmitter build() {
            if (raftClient == null) throw new RuntimeException("You must set the raftClient.");
            return new SnapshotEmitter(nodeId,
                    raftClient,
                    batchSize);
        }
    }

    /**
     * The slf4j logger to use.
     */
    private final Logger log;

    /**
     * The RaftClient to use.
     */
    private final RaftClient<ApiMessageAndVersion> raftClient;

    /**
     * The maximum number of records to put in each batch.
     */
    private final int batchSize;

    private SnapshotEmitter(
            int nodeId,
            RaftClient<ApiMessageAndVersion> raftClient,
            int batchSize
    ) {
        this.log = new LogContext("[SnapshotEmitter id=" + nodeId + "] ").logger(SnapshotEmitter.class);
        this.raftClient = raftClient;
        this.batchSize = batchSize;
    }

    @Override
    public void maybeEmit(MetadataImage image) {
        MetadataProvenance provenance = image.provenance();
        Optional<SnapshotWriter<ApiMessageAndVersion>> snapshotWriter = raftClient.createSnapshot(
            provenance.snapshotId(),
            provenance.lastContainedLogTimeMs()
        );
        if (!snapshotWriter.isPresent()) {
            log.error("Not generating {} because it already exists.", provenance.snapshotName());
            return;
        }
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter.get(), batchSize);
        try {
            image.write(writer, new ImageWriterOptions.Builder().
                    setMetadataVersion(image.features().metadataVersion()).
                    build());
            writer.close(true);
        } catch (Throwable e) {
            log.error("Encountered error while writing {}", provenance.snapshotName(), e);
            throw e;
        } finally {
            Utils.closeQuietly(writer, "RaftSnapshotWriter");
            Utils.closeQuietly(snapshotWriter.get(), "SnapshotWriter");
        }
        log.info("Successfully wrote {}", provenance.snapshotName());
    }
}
