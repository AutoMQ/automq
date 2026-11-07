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

package com.automq.stream.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class Utils {
    public static final String MAX_MERGE_READ_SPARSITY_RATE_NAME = "MERGE_READ_SPARSITY_RATE";

    public static float getMaxMergeReadSparsityRate() {
        float rate;
        try {
            rate = Float.parseFloat(System.getenv(MAX_MERGE_READ_SPARSITY_RATE_NAME));
        } catch (Exception e) {
            rate = 0.5f;
        }
        return rate;
    }

    public static void delete(Path rootFile) throws IOException {
        if (rootFile == null)
            return;
        Files.walkFileTree(rootFile, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                if (exc instanceof NoSuchFileException) {
                    if (path.equals(rootFile)) {
                        // If the root path did not exist, ignore the error and terminate;
                        return FileVisitResult.TERMINATE;
                    } else {
                        // Otherwise, just continue walking as the file might already be deleted by other threads.
                        return FileVisitResult.CONTINUE;
                    }
                }
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                // KAFKA-8999: if there's an exception thrown previously already, we should throw it
                if (exc != null) {
                    throw exc;
                }

                Files.deleteIfExists(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
