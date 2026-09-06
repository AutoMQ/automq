/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * logical meta data for a Kafka topicPartition.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticLogMeta {
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
        StringBuilder sb = new StringBuilder("ElasticLogMeta{");
        sb.append("streamMap=").append(streamMap).append(", ");

        int size = segmentMetas.size();
        sb.append("segmentMetas.size=").append(size).append(", ");
        if (size > 10) {
            List<ElasticStreamSegmentMeta> firstFiveSegmentMetas = segmentMetas.subList(0, 5);
            List<ElasticStreamSegmentMeta> lastFiveSegmentMetas = segmentMetas.subList(size - 5, size);

            sb.append("segmentMetas=[");
            for (ElasticStreamSegmentMeta meta : firstFiveSegmentMetas) {
                sb.append(meta).append(", ");
            }
            sb.append("..., ");
            for (ElasticStreamSegmentMeta meta : lastFiveSegmentMetas) {
                sb.append(meta).append(", ");
            }
            // remove the last ", "
            sb.setLength(sb.length() - 2);
            sb.append("]");
        } else {
            sb.append("segmentMetas=").append(segmentMetas);
        }

        sb.append('}');
        return sb.toString();
    }

}
