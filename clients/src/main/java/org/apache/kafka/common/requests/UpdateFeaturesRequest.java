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
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.UpdateFeaturesRequestData.FeatureUpdateKey;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult;
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

public class UpdateFeaturesRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<UpdateFeaturesRequest> {

        private final UpdateFeaturesRequestData data;

        public Builder(UpdateFeaturesRequestData data) {
            super(ApiKeys.UPDATE_FEATURES);
            this.data = data;
        }

        @Override
        public UpdateFeaturesRequest build(short version) {
            return new UpdateFeaturesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final UpdateFeaturesRequestData data;

    public UpdateFeaturesRequest(UpdateFeaturesRequestData data, short version) {
        super(ApiKeys.UPDATE_FEATURES, version);
        this.data = data;
    }

    public UpdateFeaturesRequest(Struct struct, short version) {
        super(ApiKeys.UPDATE_FEATURES, version);
        this.data = new UpdateFeaturesRequestData(struct, version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        final ApiError apiError = ApiError.fromThrowable(e);
        final UpdatableFeatureResultCollection results = new UpdatableFeatureResultCollection();
        for (FeatureUpdateKey update : this.data.featureUpdates().valuesSet()) {
            final UpdatableFeatureResult result = new UpdatableFeatureResult()
                .setFeature(update.feature())
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message());
            results.add(result);
        }
        final UpdateFeaturesResponseData responseData = new UpdateFeaturesResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResults(results);
        return new UpdateFeaturesResponse(responseData);    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    public UpdateFeaturesRequestData data() {
        return data;
    }

    public static UpdateFeaturesRequest parse(ByteBuffer buffer, short version) {
        return new UpdateFeaturesRequest(
            ApiKeys.UPDATE_FEATURES.parseRequest(version, buffer), version);
    }

    public static boolean isDeleteRequest(UpdateFeaturesRequestData.FeatureUpdateKey update) {
        return update.maxVersionLevel() < 1 && update.allowDowngrade();
    }
}
