/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package kafka.log.stream.s3.metadata;

import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;

import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Resolves Stream Objects across the published Archive boundary using one caller-owned metadata Image.
 */
final class StreamArchiveReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamArchiveReader.class);
    private static final long CORRUPTION_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final int MAX_CORRUPTION_LOG_KEYS = 1_024;
    private static final Map<String, Long> LAST_CORRUPTION_LOG_NANOS = new HashMap<>();

    private final ObjectReaderFactory objectReaderFactory;
    private final OnlineObjectFetcher onlineObjectFetcher;

    StreamArchiveReader(ObjectReaderFactory objectReaderFactory, OnlineObjectFetcher onlineObjectFetcher) {
        this.objectReaderFactory = objectReaderFactory;
        this.onlineObjectFetcher = onlineObjectFetcher;
    }

    CompletableFuture<InRangeObjects> fetch(S3StreamsMetadataImage streamsImage, S3ObjectsImage objectsImage,
        long streamId, long startOffset, long endOffset, int limit) {
        CompletableFuture<InRangeObjects> result = fetch0(streamsImage, objectsImage, streamId, startOffset,
            endOffset, limit);
        return result.whenComplete((ignored, exception) -> {
            Throwable cause = FutureUtil.cause(exception);
            if (cause instanceof ArchiveCorruptionException) {
                logCorruption(streamId, startOffset, endOffset, cause);
            }
        });
    }

    private CompletableFuture<InRangeObjects> fetch0(S3StreamsMetadataImage streamsImage, S3ObjectsImage objectsImage,
        long streamId, long startOffset, long endOffset, int limit) {
        if (invalidRequest(streamId, startOffset, endOffset, limit) || limit == 0) {
            return fetchOnline(streamsImage, objectsImage, streamId, startOffset, endOffset, limit);
        }
        S3StreamArchiveMetadata archive = streamsImage.getStreamArchiveMetadata(streamId);
        S3StreamMetadataImage stream = streamsImage.getStreamMetadata(streamId);
        if (archive == null || stream == null || startOffset < stream.startOffset()
            || startOffset >= archive.archiveEndOffset()) {
            return fetchOnline(streamsImage, objectsImage, streamId, startOffset, endOffset, limit);
        }
        long archiveEndOffset = archive.archiveEndOffset();
        long archiveTargetOffset = endOffset == ObjectUtils.NOOP_OFFSET
            ? archiveEndOffset : Math.min(endOffset, archiveEndOffset);
        ObjectStorage.ListOptions options = new ObjectStorage.ListOptions(ArchiveObjectKey.manifestPrefix(streamId))
            .startAfter(ArchiveObjectKey.startAfter(streamId, startOffset))
            .maxKeys(limit);
        return objectReaderFactory.getObjectStorage().primary().list(options).thenCompose(listedObjects -> {
            List<S3ObjectMetadata> archivedObjects = parse(streamId, startOffset, archiveTargetOffset,
                archiveEndOffset, limit, listedObjects);
            long coveredEndOffset = archivedObjects.get(archivedObjects.size() - 1).endOffset();
            if (archivedObjects.size() >= limit
                || (endOffset != ObjectUtils.NOOP_OFFSET && coveredEndOffset >= endOffset)) {
                return CompletableFuture.completedFuture(new InRangeObjects(streamId, archivedObjects));
            }
            if (coveredEndOffset < archiveTargetOffset) {
                return CompletableFuture.failedFuture(new ArchiveMissingCoverageException(String.format(
                    "Archive LIST for stream %d covered only to %d, expected %d", streamId, coveredEndOffset,
                    archiveTargetOffset)));
            }
            return fetchOnline(streamsImage, objectsImage, streamId, archiveEndOffset, endOffset,
                limit - archivedObjects.size()).thenApply(online -> combine(streamId, archiveEndOffset,
                    archivedObjects, online));
        });
    }

    private CompletableFuture<InRangeObjects> fetchOnline(S3StreamsMetadataImage streamsImage,
        S3ObjectsImage objectsImage, long streamId, long startOffset, long endOffset, int limit) {
        return onlineObjectFetcher.fetch(streamsImage, objectsImage, streamId, startOffset, endOffset, limit);
    }

    private static boolean invalidRequest(long streamId, long startOffset, long endOffset, int limit) {
        return streamId < 0 || limit < 0
            || (endOffset != ObjectUtils.NOOP_OFFSET && startOffset > endOffset);
    }

    private static List<S3ObjectMetadata> parse(long streamId, long startOffset, long targetOffset,
        long archiveEndOffset, int limit, List<ObjectStorage.ObjectInfo> listedObjects) {
        List<S3ObjectMetadata> objects = new ArrayList<>(Math.min(limit, listedObjects.size()));
        long expectedOffset = startOffset;
        for (ObjectStorage.ObjectInfo objectInfo : listedObjects) {
            if (objects.size() >= limit || (!objects.isEmpty() && expectedOffset >= targetOffset)) {
                break;
            }
            ArchiveObjectKey.ManifestKey key = parseKey(objectInfo.key());
            validateKey(key, streamId, startOffset, expectedOffset, objects.isEmpty(), archiveEndOffset);
            objects.add(ArchiveObjectKey.objectMetadata(objectInfo, key));
            expectedOffset = key.endOffset();
        }
        if (objects.isEmpty() || (expectedOffset < targetOffset && objects.size() < limit)) {
            throw new ArchiveMissingCoverageException(String.format(
                "Archive LIST for stream %d does not cover requested range [%d, %d)", streamId, startOffset,
                targetOffset));
        }
        return objects;
    }

    private static ArchiveObjectKey.ManifestKey parseKey(String objectKey) {
        try {
            return ArchiveObjectKey.parseManifestKey(objectKey);
        } catch (IllegalArgumentException e) {
            throw new ArchiveMalformedKeyException("Cannot parse Archive key " + objectKey, e);
        }
    }

    private static void validateKey(ArchiveObjectKey.ManifestKey key, long streamId, long requestedOffset,
        long expectedOffset, boolean firstObject, long archiveEndOffset) {
        if (key.streamId() != streamId) {
            throw new ArchiveMalformedKeyException("Archive key belongs to stream " + key.streamId()
                + " instead of " + streamId, null);
        }
        if (firstObject && (key.startOffset() > requestedOffset || key.endOffset() <= requestedOffset)) {
            throw new ArchiveMissingCoverageException(String.format(
                "First Archive range [%d, %d) does not cover requested offset %d",
                key.startOffset(), key.endOffset(), requestedOffset));
        }
        if (!firstObject && key.startOffset() != expectedOffset) {
            throw new ArchiveDiscontinuousRangeException(String.format(
                "Archive range starts at %d instead of continuous offset %d", key.startOffset(), expectedOffset));
        }
        if (key.startOffset() < archiveEndOffset && key.endOffset() > archiveEndOffset) {
            throw new ArchiveDiscontinuousRangeException(String.format(
                "Archive range [%d, %d) crosses published end offset %d", key.startOffset(), key.endOffset(),
                archiveEndOffset));
        }
    }

    private static InRangeObjects combine(long streamId, long archiveEndOffset,
        List<S3ObjectMetadata> archivedObjects, InRangeObjects online) {
        if (online == InRangeObjects.INVALID) {
            return online;
        }
        if (!online.objects().isEmpty() && online.startOffset() != archiveEndOffset) {
            throw new ArchiveDiscontinuousRangeException(String.format(
                "Online range starts at %d instead of published Archive end %d", online.startOffset(),
                archiveEndOffset));
        }
        List<S3ObjectMetadata> combined = new ArrayList<>(archivedObjects.size() + online.objects().size());
        combined.addAll(archivedObjects);
        combined.addAll(online.objects());
        return new InRangeObjects(streamId, combined);
    }

    private static synchronized void logCorruption(long streamId, long startOffset, long endOffset,
        Throwable cause) {
        long now = System.nanoTime();
        String key = streamId + ":" + cause.getClass().getName() + ":" + cause.getMessage();
        Long previous = LAST_CORRUPTION_LOG_NANOS.get(key);
        if (previous != null && now - previous < CORRUPTION_LOG_INTERVAL_NANOS) {
            return;
        }
        if (!LAST_CORRUPTION_LOG_NANOS.containsKey(key) && LAST_CORRUPTION_LOG_NANOS.size() >= MAX_CORRUPTION_LOG_KEYS) {
            String oldestKey = LAST_CORRUPTION_LOG_NANOS.entrySet().stream()
                .min(Map.Entry.comparingByValue()).orElseThrow().getKey();
            LAST_CORRUPTION_LOG_NANOS.remove(oldestKey);
        }
        LAST_CORRUPTION_LOG_NANOS.put(key, now);
        LOGGER.error("[FetchObjects],[ARCHIVE_CORRUPTION],streamId={} startOffset={} endOffset={}",
            streamId, startOffset, endOffset, cause);
    }

    @FunctionalInterface
    interface OnlineObjectFetcher {
        CompletableFuture<InRangeObjects> fetch(S3StreamsMetadataImage streamsImage, S3ObjectsImage objectsImage,
            long streamId, long startOffset, long endOffset, int limit);
    }
}
