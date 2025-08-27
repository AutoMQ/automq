/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.zerozone;

import kafka.server.MetadataCache;

import org.apache.kafka.common.Node;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3StreamSetObject;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

class SubscriberReplayer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberReplayer.class);
    private long loadedObjectOrderId = -1L;
    private CompletableFuture<Void> lastDataLoadCf = CompletableFuture.completedFuture(null);
    private WriteAheadLog wal;
    private RecordOffset loadedEndOffset = null;

    private final Replayer replayer;
    private final Node node;
    private final MetadataCache metadataCache;
    private final ConfirmWALProvider confirmWALProvider;

    public SubscriberReplayer(ConfirmWALProvider confirmWALProvider, Replayer replayer, Node node, MetadataCache metadataCache) {
        this.confirmWALProvider = confirmWALProvider;
        this.replayer = replayer;
        this.node = node;
        this.metadataCache = metadataCache;
    }

    public void onNewWalEndOffset(String walConfig, RecordOffset endOffset) {
        if (wal == null) {
            this.wal = confirmWALProvider.readOnly(walConfig, node.id());
        }
        if (endOffset.equals(loadedEndOffset)) {
            return;
        }
        RecordOffset startOffset = this.loadedEndOffset;
        this.loadedEndOffset = endOffset;
        if (startOffset == null) {
            return;
        }
        // The replayer will ensure the order of replay
        this.lastDataLoadCf = replayer.replay(wal, startOffset, endOffset).thenAccept(nil -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("replay {} confirm wal [{}, {})", node, startOffset, endOffset);
            }
        });
    }

    public CompletableFuture<Void> relayObject() {
        List<S3ObjectMetadata> newObjects = nextObjects().stream().filter(object -> {
            if (object.objectSize() > 200L * 1024 * 1024) {
                LOGGER.warn("The object {} is bigger than 200MiB, skip load it", object);
                return false;
            } else {
                return true;
            }
        }).collect(Collectors.toList());
        if (newObjects.isEmpty()) {
            return lastDataLoadCf;
        }
        long loadedObjectOrderId = this.loadedObjectOrderId;
        return lastDataLoadCf = lastDataLoadCf.thenCompose(nil -> replayer.replay(newObjects)).thenAccept(nil -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[LOAD_SNAPSHOT_READ_DATA],node={},loadedObjectOrderId={},newObjects={}", node, loadedObjectOrderId, newObjects);
            }
        });
    }

    public CompletableFuture<Void> replayWal() {
        return lastDataLoadCf;
    }

    public void close() {
        WriteAheadLog wal = this.wal;
        if (wal != null) {
            FutureUtil.suppress(wal::shutdownGracefully, LOGGER);
        }
    }

    private List<S3ObjectMetadata> nextObjects() {
        return nextObjects0(metadataCache, node.id(), loadedObjectOrderId, value -> loadedObjectOrderId = value);
    }

    static List<S3ObjectMetadata> nextObjects0(MetadataCache metadataCache, int nodeId, long loadedObjectOrderId,
        LongConsumer loadedObjectOrderIdUpdater) {
        return metadataCache.safeRun(image -> {
            List<S3ObjectMetadata> newObjects = new ArrayList<>();
            List<S3StreamSetObject> streamSetObjects = image.streamsMetadata().getStreamSetObjects(nodeId);
            S3ObjectsImage objectsImage = image.objectsMetadata();
            long nextObjectOrderId = loadedObjectOrderId;
            if (loadedObjectOrderId == -1L) {
                // try to load the latest 16MB data
                long size = 0;
                for (int i = streamSetObjects.size() - 1; i >= 0 && size < 16 * 1024 * 1024 && newObjects.size() < 8; i--) {
                    S3StreamSetObject sso = streamSetObjects.get(i);
                    S3Object s3object = objectsImage.getObjectMetadata(sso.objectId());
                    size += s3object.getObjectSize();
                    newObjects.add(new S3ObjectMetadata(sso.objectId(), s3object.getObjectSize(), s3object.getAttributes()));
                    nextObjectOrderId = Math.max(nextObjectOrderId, sso.orderId());
                }
            } else {
                for (int i = streamSetObjects.size() - 1; i >= 0; i--) {
                    S3StreamSetObject sso = streamSetObjects.get(i);
                    if (sso.orderId() <= loadedObjectOrderId) {
                        break;
                    }
                    S3Object s3object = objectsImage.getObjectMetadata(sso.objectId());
                    newObjects.add(new S3ObjectMetadata(sso.objectId(), s3object.getObjectSize(), s3object.getAttributes()));
                    nextObjectOrderId = Math.max(nextObjectOrderId, sso.orderId());
                }
            }
            loadedObjectOrderIdUpdater.accept(nextObjectOrderId);
            Collections.reverse(newObjects);
            return newObjects;
        });
    }
}
