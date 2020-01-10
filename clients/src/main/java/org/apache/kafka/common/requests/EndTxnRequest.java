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

import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class EndTxnRequest extends AbstractRequest {

    public final EndTxnRequestData data;

    public static class Builder extends AbstractRequest.Builder<EndTxnRequest> {
        public final EndTxnRequestData data;

        public Builder(EndTxnRequestData data) {
            super(ApiKeys.END_TXN);
            this.data = data;
        }

        @Override
        public EndTxnRequest build(short version) {
            return new EndTxnRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private EndTxnRequest(EndTxnRequestData data, short version) {
        super(ApiKeys.END_TXN, version);
        this.data = data;
    }

    public EndTxnRequest(Struct struct, short version) {
        super(ApiKeys.END_TXN, version);
        this.data = new EndTxnRequestData(struct, version);
    }

    public TransactionResult result() {
        if (data.committed())
            return TransactionResult.COMMIT;
        else
            return TransactionResult.ABORT;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public EndTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EndTxnResponse(new EndTxnResponseData()
                                      .setErrorCode(Errors.forException(e).code())
                                      .setThrottleTimeMs(throttleTimeMs)
        );
    }

    public static EndTxnRequest parse(ByteBuffer buffer, short version) {
        return new EndTxnRequest(ApiKeys.END_TXN.parseRequest(version, buffer), version);
    }
}
