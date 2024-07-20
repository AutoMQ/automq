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

package kafka.log.streamaspect;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * logical meta data for a Kafka topicPartition.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticLogMeta {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = OBJECT_MAPPER.writer();
    private static final ObjectReader READER = OBJECT_MAPPER.readerFor(ElasticLogMeta.class);

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticLogMeta.class);
    /**
     * KV map for segment file suffix -> streamId.
     */
    private Map<String, Long> streamMap = new HashMap<>();
    /**
     * segmentMeta List for this topicPartition. Each segmentMeta refers to a logical Kafka segment(including log, index, time-index, txn).
     */
    private List<ElasticStreamSegmentMeta> segmentMetas = new LinkedList<>();

    public ElasticLogMeta() {
    }

    public static ByteBuffer encode(ElasticLogMeta meta) {
        try {
            return ByteBuffer.wrap(WRITER.writeValueAsBytes(meta));
        } catch (JsonProcessingException e) {
            LOGGER.error("encode ElasticLogMeta {} failed", meta, e);
            throw new IllegalArgumentException(e);
        }
    }

    public static ElasticLogMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.UTF_8.decode(buf).toString();
        try {
            return READER.readValue(metaStr);
        } catch (JsonProcessingException e) {
            LOGGER.error("decode ElasticLogMeta {} failed", metaStr, e);
            throw new RuntimeException(e);
        }
    }

    public List<ElasticStreamSegmentMeta> getSegmentMetas() {
        return segmentMetas;
    }

    public void setSegmentMetas(List<ElasticStreamSegmentMeta> segmentMetas) {
        this.segmentMetas = segmentMetas;
    }

    public Map<String, Long> getStreamMap() {
        return streamMap;
    }

    public void setStreamMap(Map<String, Long> streamMap) {
        this.streamMap = streamMap;
    }

    @Override
    public String toString() {
        int size = segmentMetas.size();
        List<ElasticStreamSegmentMeta> lastNthSegmentMetas = segmentMetas.subList(Math.max(0, size - 5), size);
        return "ElasticLogMeta{" +
                "streamMap=" + streamMap +
                ", lastNthSegmentMetas=" + lastNthSegmentMetas +
                '}';
    }
}
