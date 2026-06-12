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

package org.apache.kafka.controller.availability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;

public final class AvailabilityCodecs {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = OBJECT_MAPPER.writer();
    private static final ObjectReader SIGNAL_READER = OBJECT_MAPPER.readerFor(BrokerAvailabilitySnapshot.class);
    private static final ObjectReader ACTION_READER = OBJECT_MAPPER.readerFor(RecoveryAction.class);
    private static final ObjectReader RESPONSE_READER = OBJECT_MAPPER.readerFor(ActionResponse.class);
    private static final ObjectReader CONTROLLER_ACTION_READER = OBJECT_MAPPER.readerFor(ControllerActionState.class);

    private AvailabilityCodecs() {
    }

    public static byte[] encodeSignal(BrokerAvailabilitySnapshot signal) {
        return encode(signal);
    }

    public static BrokerAvailabilitySnapshot decodeSignal(byte[] raw) {
        return decode(raw, SIGNAL_READER);
    }

    public static byte[] encodeAction(RecoveryAction action) {
        return encode(action);
    }

    public static RecoveryAction decodeAction(byte[] raw) {
        return decode(raw, ACTION_READER);
    }

    public static byte[] encodeResponse(ActionResponse response) {
        return encode(response);
    }

    public static ActionResponse decodeResponse(byte[] raw) {
        return decode(raw, RESPONSE_READER);
    }

    public static byte[] encodeControllerAction(ControllerActionState state) {
        return encode(state);
    }

    public static ControllerActionState decodeControllerAction(byte[] raw) {
        return decode(raw, CONTROLLER_ACTION_READER);
    }

    private static byte[] encode(Object value) {
        try {
            return WRITER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static <T> T decode(byte[] raw, ObjectReader reader) {
        try {
            return reader.readValue(raw);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
