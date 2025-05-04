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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class LocalFileObjectStorage implements ObjectStorage {
    public static final short BUCKET_ID = -2;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileObjectStorage.class);
    private final BucketURI bucketURI;
    private final Path dataParentPath;
    private final String dataParentPathStr;
    private final String atomicWriteParentPathStr;
    private final AtomicLong atomicWriteCounter = new AtomicLong(0);
    // thread-safe is guarded by synchronized(availableSpace)
    final AtomicLong availableSpace = new AtomicLong();
    final Queue<WriteTask> waitingTasks = new LinkedBlockingQueue<>();
    private final ExecutorService ioExecutor = Threads.newFixedThreadPoolWithMonitor(8, "LOCAL_FILE_OBJECT_STORAGE_IO", true, LOGGER);

    public LocalFileObjectStorage(BucketURI bucketURI) {
        if (bucketURI.bucketId() != BUCKET_ID) {
            throw new IllegalArgumentException("bucketId must be -2");
        }
        this.bucketURI = bucketURI;
        this.dataParentPathStr = bucketURI.bucket() + File.separator + "data";
        this.dataParentPath = Path.of(dataParentPathStr);
        this.atomicWriteParentPathStr = bucketURI.bucket() + File.separator + "atomic";

        try {
            if (!dataParentPath.toFile().isDirectory()) {
                Files.createDirectories(dataParentPath);
            }
            availableSpace.set(new File(dataParentPathStr).getFreeSpace() - 2L * 1024 * 1024 * 1024);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(atomicWriteParentPathStr))) {
            for (Path file : stream) {
                if (Files.isRegularFile(file)) {
                    Files.delete(file);
                }
            }
        } catch (IOException ignored) {
        }
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
        Path path = dataPath(objectPath);
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
                cf.completeExceptionally(new ObjectNotExistException());
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
            Path path = dataPath(prefix);
            String pathPrefix = path.toString();
            if (!Files.isDirectory(path)) {
                path = path.getParent();
            }
            List<ObjectInfo> list = new ArrayList<>();
            if (path != null && Files.exists(path)) {
                Files.walkFileTree(path, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (!Files.isDirectory(file)) {
                            String filePathStr = file.toString();
                            if (filePathStr.startsWith(pathPrefix)) {
                                list.add(new ObjectInfo(bucketId(), filePathStr.substring(dataParentPathStr.length() + 1), attrs.creationTime().toMillis(), attrs.size()));
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            cf.complete(list);
        } catch (Throwable e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ioExecutor.submit(() -> {
            long size = 0;
            try {
                for (ObjectPath objectPath : objectPaths) {
                    Path dataPath = dataPath(objectPath.key());
                    long fileSize = fileSize(dataPath);
                    deleteFileAndEmptyParents(dataPath);
                    size += fileSize;
                }
                cf.complete(null);
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            } finally {
                freeSpace(size);
            }
        });
        return cf;
    }

    @Override
    public short bucketId() {
        return bucketURI.bucketId();
    }

    private Path atomicWritePath() {
        return Path.of(atomicWriteParentPathStr + File.separator + atomicWriteCounter.incrementAndGet());
    }

    private Path dataPath(String objectPath) {
        return Path.of(dataParentPathStr + File.separator + objectPath);
    }

    public void deleteFileAndEmptyParents(Path filePath) throws IOException {
        if (!Files.deleteIfExists(filePath)) {
            return;
        }

        Path parentDir = filePath.getParent();

        while (parentDir != null && !parentDir.equals(dataParentPath)) {
            try {
                Files.delete(parentDir);
                parentDir = parentDir.getParent();
            } catch (DirectoryNotEmptyException | NoSuchFileException e) {
                break;
            }
        }
    }

    private void acquireSpace(int size, Runnable task) {
        boolean acquired = false;
        synchronized (availableSpace) {
            if (availableSpace.get() > size) {
                availableSpace.addAndGet(-size);
                acquired = true;
            } else {
                waitingTasks.add(new WriteTask(size, task));
                LOGGER.info("[LOCAL_FILE_OBJECT_STORAGE_FULL]");
            }
        }
        if (acquired) {
            task.run();
        }
    }

    private void freeSpace(long size) {
        synchronized (availableSpace) {
            availableSpace.addAndGet(size);
            for (;;) {
                WriteTask task = waitingTasks.peek();
                if (task == null) {
                    break;
                }
                if (task.writeSize > availableSpace.get()) {
                    break;
                }
                waitingTasks.poll();
                availableSpace.addAndGet(-task.writeSize);
                FutureUtil.suppress(task.task::run, LOGGER);
            }
        }
    }

    class LocalFileWriter implements Writer {
        private final Path atomicWritePath;
        private final Path dataPath;
        private final FileChannel fileChannel;
        private Throwable cause;
        private final List<CompletableFuture<Void>> writeCfList = new LinkedList<>();
        private long nextWritePosition = 0;

        public LocalFileWriter(String objectPath) {
            this.atomicWritePath = atomicWritePath();
            this.dataPath = dataPath(objectPath);
            FileChannel fileChannel = null;
            try {
                for (int i = 0; i < 2; i++) {
                    try {
                        fileChannel = FileChannel.open(atomicWritePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                    } catch (NoSuchFileException e) {
                        Path parent = atomicWritePath.getParent();
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
            int dataSize = data.readableBytes();
            nextWritePosition += dataSize;
            acquireSpace(dataSize, () -> ioExecutor.execute(() -> {
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
            }));
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
                        try {
                            Files.move(atomicWritePath, dataPath, StandardCopyOption.REPLACE_EXISTING);
                        } catch (NoSuchFileException e) {
                            Path parent = dataPath.getParent();
                            if (parent != null) {
                                Files.createDirectories(parent);
                            }
                            Files.move(atomicWritePath, dataPath, StandardCopyOption.REPLACE_EXISTING);
                        }
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

    private static long fileSize(Path filePath) {
        try {
            return Files.size(filePath);
        } catch (IOException e) {
            return 0;
        }
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

    static class WriteTask {
        final long writeSize;
        // The task should be a none blocking task
        final Runnable task;

        public WriteTask(long writeSize, Runnable task) {
            this.writeSize = writeSize;
            this.task = task;
        }
    }
}
