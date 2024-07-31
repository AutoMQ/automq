/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Partition dimension metadata, record the recover checkpoint, clean offset...
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticPartitionMeta {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = OBJECT_MAPPER.writer();
    private static final ObjectReader READER = OBJECT_MAPPER.readerFor(ElasticPartitionMeta.class);

    /**
     * The start offset of this topicPartition.
     * It may be altered after log cleaning.
     */
    @JsonProperty("s")
    private Long startOffset;

    /**
     * The offset of the last cleaned record.
     */
    @JsonProperty("c")
    private Long cleanerOffset;

    @JsonProperty("r")
    private Long recoverOffset;
    @JsonProperty("cs")
    private boolean cleanedShutdown;

    @SuppressWarnings("unused") // used by jackson
    public ElasticPartitionMeta() {
    }

    public ElasticPartitionMeta(Long startOffset, Long cleanerOffset, Long recoverOffset) {
        this.startOffset = startOffset;
        this.cleanerOffset = cleanerOffset;
        this.recoverOffset = recoverOffset;
    }

    public static ByteBuffer encode(ElasticPartitionMeta meta) {
        try {
            return ByteBuffer.wrap(WRITER.writeValueAsBytes(meta));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static ElasticPartitionMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.UTF_8.decode(buf).toString();
        try {
            return READER.readValue(metaStr);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Long startOffset) {
        this.startOffset = startOffset;
    }

    public Long getCleanerOffset() {
        return cleanerOffset;
    }

    public void setCleanerOffset(Long cleanerOffset) {
        this.cleanerOffset = cleanerOffset;
    }

    public Long getRecoverOffset() {
        return recoverOffset;
    }

    public void setRecoverOffset(Long recoverOffset) {
        this.recoverOffset = recoverOffset;
    }

    public boolean getCleanedShutdown() {
        return this.cleanedShutdown;
    }

    public void setCleanedShutdown(boolean cleanedShutdown) {
        this.cleanedShutdown = cleanedShutdown;
    }

    @Override
    public String toString() {
        return "ElasticPartitionMeta{" +
                "startOffset=" + startOffset +
                ", cleanerOffset=" + cleanerOffset +
                ", recoverOffset=" + recoverOffset +
                ", cleanedShutdown=" + cleanedShutdown +
                '}';
    }
}
