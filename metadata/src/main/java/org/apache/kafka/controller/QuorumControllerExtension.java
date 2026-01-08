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

package org.apache.kafka.controller;

import org.apache.kafka.common.message.DescribeLicenseRequestData;
import org.apache.kafka.common.message.DescribeLicenseResponseData;
import org.apache.kafka.common.message.ExportClusterManifestRequestData;
import org.apache.kafka.common.message.ExportClusterManifestResponseData;
import org.apache.kafka.common.message.UpdateLicenseRequestData;
import org.apache.kafka.common.message.UpdateLicenseResponseData;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface QuorumControllerExtension {
    QuorumControllerExtension NOOP = new QuorumControllerExtension() { };

    default boolean replay(MetadataRecordType type, ApiMessage message, Optional<OffsetAndEpoch> snapshotId, long batchLastOffset) {
        return false;
    }

    default CompletableFuture<UpdateLicenseResponseData> updateLicense(
        ControllerRequestContext context, UpdateLicenseRequestData request) {
        return null;
    }

    default Supplier<DescribeLicenseResponseData> describeLicenseSupplier(
        ControllerRequestContext context, DescribeLicenseRequestData request) {
        return null;
    }

    default Supplier<ExportClusterManifestResponseData> exportClusterManifestSupplier(
        ControllerRequestContext context, ExportClusterManifestRequestData request) {
        return null;
    }

    default List<ApiMessageAndVersion> getActivationRecords() {
        return Collections.emptyList();
    }
}
