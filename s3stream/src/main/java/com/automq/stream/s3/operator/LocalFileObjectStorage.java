/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class LocalFileObjectStorage implements ObjectStorage {
    public static final short BUCKET_ID = -2;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileObjectStorage.class);
    private final BucketURI bucketURI;
    private final Path parentPath;
    private final String parentPathStr;
    private final ExecutorService ioExecutor = Threads.newFixedThreadPoolWithMonitor(8, "LOCAL_FILE_OBJECT_STORAGE_IO", true, LOGGER);

    public LocalFileObjectStorage(BucketURI bucketURI) {
        if (bucketURI.bucketId() != BUCKET_ID) {
            throw new IllegalArgumentException("bucketId must be -2");
        }
        this.bucketURI = bucketURI;
        this.parentPathStr = bucketURI.bucket();
        this.parentPath = Path.of(parentPathStr);
    }

    @Override
    public boolean readinessCheck() {
        return true;
    }

    @Override
    public void close() {

    }

    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        options.bucketId(bucketId());
        return new LocalFileWriter(objectPath);
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
        CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
        Path path = path(objectPath);
        ioExecutor.submit(() -> {
            long position = start;
            try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                byte[] bytes;
                if (end == -1) {
                    bytes = new byte[(int) (fileChannel.size() - start)];
                } else {
                    bytes = new byte[(int) (end - start)];
                }
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    int readSize = fileChannel.read(buffer, position);
                    if (readSize == -1) {
                        cf.completeExceptionally(new IllegalArgumentException(String.format("rangeRead %s [%s, %s) out of bound [0, %s)",
                            objectPath, start, end, fileChannel.size())));
                        return;
                    }
                    position += readSize;
                }
                cf.complete(Unpooled.wrappedBuffer(bytes));
            } catch (NoSuchFileException e) {
                cf.completeExceptionally(new ObjectNotExistException(null));
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        CompletableFuture<List<ObjectInfo>> cf = new CompletableFuture<>();
        try {
            Path path = path(prefix);
            String fileNameMatch = null;
            if (!Files.isDirectory(path)) {
                fileNameMatch = Optional.ofNullable(path.getFileName()).map(Path::toString).orElse(null);
                path = path.getParent();
            }
            List<ObjectInfo> list = new ArrayList<>();
            if (path != null && Files.exists(path)) {
                String finalFileNameMatch = fileNameMatch;
                Files.walkFileTree(path, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (!Files.isDirectory(file)) {
                            Path fileName = file.getFileName();
                            if (fileName != null && (finalFileNameMatch == null || fileName.toString().startsWith(finalFileNameMatch))) {
                                list.add(new ObjectInfo(bucketId(), file.toString().substring(parentPathStr.length() + 1), attrs.creationTime().toMillis(), attrs.size()));
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        return FileVisitResult.CONTINUE;
                    }
                });
                cf.complete(list);
            }
        } catch (Throwable e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ioExecutor.submit(() -> {
            try {
                for (ObjectPath objectPath : objectPaths) {
                    deleteFileAndEmptyParents(path(objectPath.key()));
                }
                cf.complete(null);
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }

    @Override
    public short bucketId() {
        return bucketURI.bucketId();
    }

    private Path path(String objectPath) {
        return Path.of(parentPathStr + File.separator + objectPath);
    }

    public void deleteFileAndEmptyParents(Path filePath) throws IOException {
        if (!Files.deleteIfExists(filePath)) {
            return;
        }

        Path parentDir = filePath.getParent();

        while (parentDir != null && !parentDir.equals(parentPath)) {
            try {
                Files.delete(parentDir);
                parentDir = parentDir.getParent();
            } catch (DirectoryNotEmptyException | NoSuchFileException e) {
                break;
            }
        }
    }

    class LocalFileWriter implements Writer {
        private final FileChannel fileChannel;
        private Throwable cause;
        private final List<CompletableFuture<Void>> writeCfList = new LinkedList<>();
        private long nextWritePosition = 0;

        public LocalFileWriter(String objectPath) {
            Path path = path(objectPath);
            FileChannel fileChannel = null;
            try {
                for (int i = 0; i < 2; i++) {
                    try {
                        fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                    } catch (NoSuchFileException e) {
                        Path parent = path.getParent();
                        if (parent != null) {
                            Files.createDirectories(parent);
                        }
                    }
                }
                if (fileChannel == null) {
                    throw new IllegalStateException("expect file channel create success");
                }
            } catch (Throwable e) {
                cause = e;
            }
            this.fileChannel = fileChannel;
        }

        @Override
        public CompletableFuture<Void> write(ByteBuf data) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            CompletableFuture<Void> retCf = cf.whenComplete((v, ex) -> data.release());
            if (cause != null) {
                cf.completeExceptionally(cause);
                return retCf;
            }
            long startWritePosition = nextWritePosition;
            nextWritePosition += data.readableBytes();
            ioExecutor.execute(() -> {
                long position = startWritePosition;
                try {
                    ByteBuffer[] buffers = data.nioBuffers();
                    for (ByteBuffer buf : buffers) {
                        while (buf.hasRemaining()) {
                            position += fileChannel.write(buf, position);
                        }
                    }
                    cf.complete(null);
                } catch (Throwable e) {
                    cf.completeExceptionally(e);
                }
            });
            writeCfList.add(retCf);
            return retCf;
        }

        @Override
        public void copyOnWrite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBatchingPart() {
            return false;
        }

        @Override
        public CompletableFuture<Void> close() {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            CompletableFuture.allOf(writeCfList.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                    return;
                }
                ioExecutor.execute(() -> {
                    try (fileChannel) {
                        fileChannel.force(true);
                        cf.complete(null);
                    } catch (Throwable e) {
                        cf.completeExceptionally(e);
                    }
                });
            });
            return cf;
        }

        @Override
        public CompletableFuture<Void> release() {
            return CompletableFuture.allOf(writeCfList.toArray(new CompletableFuture[0]));
        }

        @Override
        public short bucketId() {
            return LocalFileObjectStorage.this.bucketId();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private BucketURI bucketURI;

        public Builder bucket(BucketURI bucketURI) {
            this.bucketURI = bucketURI;
            return this;
        }

        public LocalFileObjectStorage build() {
            return new LocalFileObjectStorage(bucketURI);
        }
    }
}
