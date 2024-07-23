/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.controller.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;

public class NodeMetadataCodec {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = OBJECT_MAPPER.writer();
    private static final ObjectReader READER = OBJECT_MAPPER.readerFor(NodeMetadata.class);

    public static byte[] encode(NodeMetadata nodeMetadata) {
        try {
            return WRITER.writeValueAsBytes(nodeMetadata);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static NodeMetadata decode(byte[] raw) {
        try {
            return READER.readValue(raw);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
