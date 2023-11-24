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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamReader {
    private final S3Operator operator;
    private final ObjectManager objectManager;
    private final ObjectReaderLRUCache objectReaders;

    public StreamReader(int maxObjectReaderSize, S3Operator operator, ObjectManager objectManager) {
        this.operator = operator;
        this.objectManager = objectManager;
        this.objectReaders = new ObjectReaderLRUCache(maxObjectReaderSize);
    }

    //TODO: limit concurrent object read number
    public void read(long streamId, long startOffset, long endOffset, int maxBytes) {
        CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, startOffset, endOffset, 2);

    }

    private List<ObjectReader.DataBlockIndex> getDataBlockIndexList(long streamId, long startOffset, long endOffset, int maxBytes) {
        return null;
    }
}
