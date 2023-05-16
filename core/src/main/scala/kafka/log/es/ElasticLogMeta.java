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

package kafka.log.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// TODO: save meta to stream periodically. save meta when broker shutdown.

/**
 * logical meta data for a Kafka topicPartition.
 */
public class ElasticLogMeta {
    /**
     * KV map for segment file suffix -> streamId.
     */
    private Map<String, Long> streams = new HashMap<>();
    /**
     * segment list for this topicPartition. Each item refers to a logical Kafka segment(including log, index, time-index, txn).
     */
    private List<ElasticStreamSegmentMeta> segments = new LinkedList<>();
    private List<ElasticStreamSegmentMeta> cleanedSegments = new LinkedList<>();

    public ElasticLogMeta() {}

    public static ByteBuffer encode(ElasticLogMeta meta) {
        ObjectMapper om = new ObjectMapper();
        try {
            String str = om.writeValueAsString(meta);
            return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static ElasticLogMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.UTF_8.decode(buf).toString();
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(metaStr, ElasticLogMeta.class);
        } catch (JsonProcessingException e) {
            // TODO: throw a better exception
            throw new RuntimeException(e);
        }
    }

    public List<ElasticStreamSegmentMeta> getSegments() {
        return segments;
    }

    public void setSegments(List<ElasticStreamSegmentMeta> segments) {
        this.segments = segments;
    }

    public Map<String, Long> getStreams() {
        return streams;
    }

    public void setStreams(Map<String, Long> streams) {
        this.streams = streams;
    }

    public void putStream(String name, long streamId) {
        streams.put(name, streamId);
    }

    public List<ElasticStreamSegmentMeta> getCleanedSegments() {
        return cleanedSegments;
    }

    public void setCleanedSegments(List<ElasticStreamSegmentMeta> cleanedSegments) {
        this.cleanedSegments = cleanedSegments;
    }

    @Override
    public String toString() {
        return "ElasticLogMeta{" +
                "streams=" + streams +
                ", segments=" + segments +
                '}';
    }
}
