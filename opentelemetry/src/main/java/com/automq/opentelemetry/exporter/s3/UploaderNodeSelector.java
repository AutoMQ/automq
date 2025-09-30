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

package com.automq.opentelemetry.exporter.s3;

/**
 * An interface for determining which node should be responsible for uploading metrics.
 * This abstraction allows different implementations of uploader node selection strategies.
 */
public interface UploaderNodeSelector {
    
    /**
     * Determines if the current node should be responsible for uploading metrics.
     * 
     * @return true if the current node should upload metrics, false otherwise.
     */
    boolean isPrimaryUploader();
    
    /**
     * Creates a default UploaderNodeSelector based on static configuration.
     * 
     * @param isPrimaryUploader a static boolean value indicating whether this node is the primary uploader
     * @return a UploaderNodeSelector that returns the static value
     */
    static UploaderNodeSelector staticSelector(boolean isPrimaryUploader) {
        return () -> isPrimaryUploader;
    }
}
