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

import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.SendBuilder;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

public class EnvelopeResponse extends AbstractResponse {

    private final EnvelopeResponseData data;

    public EnvelopeResponse(ByteBuffer responseData,
                            Errors error) {
        this.data = new EnvelopeResponseData()
                        .setResponseData(responseData)
                        .setErrorCode(error.code());
    }

    public EnvelopeResponse(Errors error) {
        this(null, error);
    }

    public EnvelopeResponse(EnvelopeResponseData data) {
        this.data = data;
    }

    public ByteBuffer responseData() {
        return data.responseData();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public EnvelopeResponseData data() {
        return data;
    }

    @Override
    protected Send toSend(String destination, ResponseHeader header, short apiVersion) {
        return SendBuilder.buildResponseSend(destination, header, this.data, apiVersion);
    }

}
