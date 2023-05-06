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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Partition dimension metadata, record the recover checkpoint, clean offset...
 */
public class ElasticPartitionMeta {
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

    ElasticPartitionMeta(Long startOffset, Long cleanerOffset) {
        this.startOffset = startOffset;
        this.cleanerOffset = cleanerOffset;
    }

    public static ByteBuffer encode(ElasticPartitionMeta meta) {
        ObjectMapper om = new ObjectMapper();
        try {
            String str = om.writeValueAsString(meta);
            return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static ElasticPartitionMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.UTF_8.decode(buf).toString();
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(metaStr, ElasticPartitionMeta.class);
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
}
