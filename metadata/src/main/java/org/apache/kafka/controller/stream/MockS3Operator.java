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

package org.apache.kafka.controller.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.metadata.stream.S3Object;

public class MockS3Operator implements S3Operator {

    private final Map<String/*objectKey*/, S3Object> objects = new HashMap<>();

    @Override
    public CompletableFuture<Boolean> delete(String objectKey) {
        return delele(new String[]{objectKey});
    }

    @Override
    public CompletableFuture<Boolean> delele(String[] objectKeys) {
        return CompletableFuture.completedFuture(false);
    }
}
