/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.controller.stream.lifecycle;

import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.CollectionHelper;
import com.automq.stream.utils.FutureUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.stream.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ObjectCleaner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectCleaner.class);

    private final ObjectStorage objectStorage;

    private final AtomicLong deleteObjectCounter = new AtomicLong();
    private final AtomicLong deleteCompositeObjectCounter = new AtomicLong();
    private final AtomicLong deleteLinkedObjectCounter = new AtomicLong();
    private final QuorumController quorumController;

    public ObjectCleaner(ObjectStorage objectStorage,
                         QuorumController quorumController) {
        this.objectStorage = objectStorage;
        this.quorumController = quorumController;
    }

    /**
     * this method handle delete both normal object and composite object.
     * when delete object finish successfully the callback notifyS3ObjectDeleted will be called.
     */
    public CompletableFuture<Void> clean(List<S3Object> objects) {
        TimerUtil timerUtil = new TimerUtil();
        long originDeleteObjectNumber = deleteObjectCounter.get();
        long originDeleteCompositeObjectNumber = deleteCompositeObjectCounter.get();
        long originDeleteLinkedObjectNumber = deleteLinkedObjectCounter.get();

        List<S3Object> deepDeleteCompositeObjects = new LinkedList<>();
        List<S3Object> shallowDeleteObjects = new ArrayList<>(objects.size());

        for (S3Object object : objects) {
            ObjectAttributes attributes = ObjectAttributes.from(object.getAttributes());
            if (attributes.deepDelete() && attributes.type() == ObjectAttributes.Type.Composite) {
                deepDeleteCompositeObjects.add(object);
            } else {
                shallowDeleteObjects.add(object);
            }
        }

        LOGGER.info("Start objectCleaner task, " +
                "shallowDeleteObjects={}, deepDeleteCompositeObjects={}",
            shallowDeleteObjects.size(), deepDeleteCompositeObjects.size());

        List<CompletableFuture<Void>> cfList = new LinkedList<>();
        // Delete the objects
        batchDelete(shallowDeleteObjects, this::shallowlyDelete, cfList);
        // Delete the composite object and it's linked objects
        batchDelete(deepDeleteCompositeObjects, this::deepDelete, cfList);

        return CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0]))
            .whenComplete((res, e) -> {
                long deleteObject = deleteObjectCounter.get() - originDeleteObjectNumber;
                long deleteCompositeObject = deleteCompositeObjectCounter.get() - originDeleteCompositeObjectNumber;
                long deleteLinkedObject = deleteLinkedObjectCounter.get() - originDeleteLinkedObjectNumber;

                long cost = timerUtil.elapsedAs(TimeUnit.MILLISECONDS);
                if (e != null) {
                    LOGGER.error("Fail to do objectCleaner task cost={}ms, deleteObject={}, " +
                            "deleteCompositeObject={}, deleteLinkedObject={}",
                        cost, deleteObject, deleteCompositeObject, deleteLinkedObject, e);
                } else {
                    LOGGER.info("ObjectCleaner task finished cost={}ms, deleteObject={}, " +
                            "deleteCompositeObject={}, deleteLinkedObject={}",
                        cost, deleteObject, deleteCompositeObject, deleteLinkedObject);
                }
            });
    }

    private void batchDelete(List<S3Object> objects,
                             Function<List<S3Object>, CompletableFuture<Void>> deleteFunc,
                             List<CompletableFuture<Void>> cfList) {
        if (objects.isEmpty()) {
            return;
        }
        CollectionHelper.groupListByBatchSizeAsStream(objects, AwsObjectStorage.AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER)
            .map(deleteFunc)
            .forEach(cfList::add);
    }

    private CompletableFuture<Void> shallowlyDelete(List<S3Object> s3objects) {
        List<ObjectStorage.ObjectPath> objectPaths = s3objects.stream()
            .map(o -> new ObjectStorage.ObjectPath(o.bucket(), o.getObjectKey()))
            .collect(Collectors.toList());

        deleteObjectCounter.addAndGet(objectPaths.size());

        return objectStorage.delete(objectPaths)
            .exceptionally(e -> {
                LOGGER.error("Failed to delete the S3Object from S3, objectKeys: {}",
                    objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.joining(",")), e);
                return null;
            }).thenAccept(rst -> {
                List<Long> deletedObjectIds = s3objects.stream().map(S3Object::getObjectId).collect(Collectors.toList());
                notifyS3ObjectDeleted(deletedObjectIds);
            });
    }

    private CompletableFuture<Void> handleBatchDeleteCompositeObjects(
        List<ObjectStorage.ObjectPath> linkedObjectBatch,
        List<S3ObjectMetadata> compositeObjectMetadataBatch,
        Map<S3ObjectMetadata, List<ObjectStorage.ObjectPath>> compositeLinkedObjectPath
    ) {
        // merge linkedObject and compositeObject together
        List<ObjectStorage.ObjectPath> compositePathBatch = Streams
            .concat(
                compositeObjectMetadataBatch.stream().map(m -> new ObjectStorage.ObjectPath(m.bucket(), m.objectId())),
                linkedObjectBatch.stream()
            )
            .collect(Collectors.toList());

        LOGGER.info("delete object number linkedObjectBatch={}, compositePathBatch={}",
            linkedObjectBatch.size(), compositeObjectMetadataBatch.size());

        // delete all linkedObject and compositeObject

        deleteLinkedObjectCounter.addAndGet(linkedObjectBatch.size());

        // note that objectStorage may split internally which won't be atomic delete the whole batch.
        return objectStorage.delete(compositePathBatch)
            .thenApply(__ -> {
                List<Long> deletedObjectIds = new ArrayList<>(compositeObjectMetadataBatch.size());

                // print log for all deleted composite object
                compositeObjectMetadataBatch.forEach(m -> {
                    List<ObjectStorage.ObjectPath> objectPaths = compositeLinkedObjectPath.get(m);
                    List<String> objectPath = objectPaths.stream()
                        .map(o -> o.bucketId() + "/" + o.getObjectId()).collect(Collectors.toList());

                    LOGGER.info("Delete composite object {}/{} success, linked objects number {} linked objects: {}",
                        ObjectAttributes.from(m.attributes()).bucket(), m.objectId(),
                        objectPaths.size(), objectPath);

                    compositeLinkedObjectPath.remove(m);

                    deletedObjectIds.add(m.objectId());
                });

                notifyS3ObjectDeleted(deletedObjectIds);
                return null;
            });
    }

    private CompletableFuture<Void> deepDelete(List<S3Object> s3Objects) {
        return deepDeleteCompositeObject(s3Objects, CompositeObject::getLinkedObjectPath);
    }

    @VisibleForTesting
    protected CompletableFuture<Void> deepDeleteCompositeObject(
        List<S3Object> s3Objects,
        BiFunction<S3ObjectMetadata, ObjectStorage,
            CompletableFuture<Map.Entry<S3ObjectMetadata, List<ObjectStorage.ObjectPath>>>>
            getLinkedObjectPathFunction
    ) {
        if (s3Objects.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        deleteCompositeObjectCounter.addAndGet(s3Objects.size());

        AtomicLong doneFutureNumber = new AtomicLong(s3Objects.size());

        // when get linkedObjectPath finished the result will go into this queue.
        LinkedBlockingQueue<Map.Entry<S3ObjectMetadata, List<ObjectStorage.ObjectPath>>> getLinkedObjectFinishQueue =
            new LinkedBlockingQueue<>();

        // get all composite object linked object
        for (S3Object object : s3Objects) {
            S3ObjectMetadata metadata = new S3ObjectMetadata(object.getObjectId(), object.getAttributes());

            CompletableFuture<Map.Entry<S3ObjectMetadata, List<ObjectStorage.ObjectPath>>> linkedObjectPath =
                getLinkedObjectPathFunction.apply(metadata, objectStorage);

            linkedObjectPath.whenComplete((res, e) -> {
                if (e != null) {
                    LOGGER.error("Fail to get linkedObject for composite object {}/{}",
                        ObjectAttributes.from(metadata.attributes()).bucket(), metadata.objectId(), e);
                    doneFutureNumber.decrementAndGet();
                } else {
                    getLinkedObjectFinishQueue.add(res);
                }
            });
        }

        CompletableFuture<Void> batchDeleteCompositeObjectFuture = new CompletableFuture<>();

        // this run in forkJoin commonPool

        // poll from queue and batch total deleteObjects 1000 keys per batch.
        CompletableFuture.runAsync(() -> {
            List<ObjectStorage.ObjectPath> linkedObjectBatch = new ArrayList<>(1000);
            List<S3ObjectMetadata> compositeObjectMetadataBatch = new ArrayList<>();
            Map<S3ObjectMetadata, List<ObjectStorage.ObjectPath>> compositeLinkedObjectPath = new ConcurrentHashMap<>();

            final List<CompletableFuture<Void>> deleteCfList = new ArrayList<>();

            while (true) {
                Map.Entry<S3ObjectMetadata, List<ObjectStorage.ObjectPath>> item = null;
                try {
                    item = getLinkedObjectFinishQueue.poll(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (item == null) {
                    LOGGER.warn("wait for composite object get linkedObjectPath timeout. " +
                        "left for doneFutureNumber={}", doneFutureNumber.get());
                    continue;
                }

                S3ObjectMetadata metadata = item.getKey();
                List<ObjectStorage.ObjectPath> linkedObjectPath = item.getValue();
                compositeLinkedObjectPath.put(metadata, linkedObjectPath);

                compositeObjectMetadataBatch.add(metadata);
                linkedObjectBatch.addAll(linkedObjectPath);


                if (linkedObjectBatch.size() >= 1000) {
                    deleteCfList.add(handleBatchDeleteCompositeObjects(linkedObjectBatch,
                        compositeObjectMetadataBatch,
                        compositeLinkedObjectPath));

                    linkedObjectBatch = new ArrayList<>(1000);
                    compositeObjectMetadataBatch = new ArrayList<>();
                }

                if (doneFutureNumber.decrementAndGet() == 0) {
                    break;
                }
            }

            if (!linkedObjectBatch.isEmpty()) {
                deleteCfList.add(handleBatchDeleteCompositeObjects(linkedObjectBatch,
                    compositeObjectMetadataBatch,
                    compositeLinkedObjectPath));
            }

            FutureUtil.propagate(CompletableFuture.allOf(deleteCfList.toArray(new CompletableFuture[0])),
                batchDeleteCompositeObjectFuture);
        });

        return batchDeleteCompositeObjectFuture;
    }

    private void notifyS3ObjectDeleted(List<Long> deletedObjectIds) {
        // notify the controller an objects deletion event to drive the removal of the objects
        ControllerRequestContext ctx = new ControllerRequestContext(
            null, null, OptionalLong.empty());
        quorumController.notifyS3ObjectDeleted(ctx, deletedObjectIds).whenComplete((ignore, exp) -> {
            if (exp != null) {
                LOGGER.error("Failed to notify the controller the S3Object deletion event, objectIds: {}",
                    Arrays.toString(deletedObjectIds.toArray()), exp);
            }
        });
    }
}
