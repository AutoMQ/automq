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

package com.automq.log.uploader;

import com.automq.log.uploader.selector.LogUploaderNodeSelector;
import com.automq.stream.s3.operator.ObjectStorage;

public interface S3LogConfig {
    boolean isEnabled();

    String clusterId();

    int nodeId();

    ObjectStorage objectStorage();

    LogUploaderNodeSelector nodeSelector();

    default boolean isPrimaryUploader() {
        LogUploaderNodeSelector selector = nodeSelector();
        return selector != null && selector.isPrimaryUploader();
    }
}
