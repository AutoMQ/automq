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
 * Multi-part object writer.
 */
public interface Writer {
    /**
     * Write a part of the object. The parts will parallel upload to S3.
     *
     * @param part object part.
     */
    void write(ByteBuf part);

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
