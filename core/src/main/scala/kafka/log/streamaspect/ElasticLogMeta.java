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

package kafka.log.streamaspect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * logical meta data for a Kafka topicPartition.
 */
public class ElasticLogMeta {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ElasticLogMeta.class);
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
        ObjectMapper om = new ObjectMapper();
        try {
            String str = om.writeValueAsString(meta);
            return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            LOGGER.error("encode ElasticLogMeta {} failed", meta, e);
            throw new IllegalArgumentException(e);
        }
    }

    public static ElasticLogMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.UTF_8.decode(buf).toString();
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(metaStr, ElasticLogMeta.class);
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
