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

package kafka.log.s3.operator;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

/**
 * Multipart object writer.
 * <p>
 *     Writer should ensure that a part, even with size smaller than {@link Writer#MIN_PART_SIZE}, can still be uploaded.
 *     For other S3 limits, it is upper layer's responsibility to prevent reaching the limits.
 */
public interface Writer {
    /**
     * The max number of parts. It comes from the limit of S3 multipart upload.
     */
    int MAX_PART_COUNT = 10000;
    /**
     * The max size of a part, i.e. 5GB. It comes from the limit of S3 multipart upload.
     */
    long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;
    /**
     * The min size of a part, i.e. 5MB. It comes from the limit of S3 multipart upload.
     * Note that the last part can be smaller than this size.
     */
    int MIN_PART_SIZE = 5 * 1024 * 1024;
    /**
     * The max size of an object, i.e. 5TB. It comes from the limit of S3 object size.
     */
    long MAX_OBJECT_SIZE = 5L * 1024 * 1024 * 1024 * 1024;

    /**
     * Write a part of the object. The parts will parallel upload to S3.
     *
     * @param part object part.
     */
    CompletableFuture<Void> write(ByteBuf part);

    /**
     * Copy a part of the object.
     *
     * @param sourcePath source object path.
     * @param start      start position of the source object.
     * @param end        end position of the source object.
     */
    void copyWrite(String sourcePath, long start, long end);

    /**
     * Complete the object.
     */
    CompletableFuture<Void> close();

}
