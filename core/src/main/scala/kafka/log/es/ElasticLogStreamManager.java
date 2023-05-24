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

import java.util.Collections;
import com.automq.elasticstream.client.api.Stream;
import com.automq.elasticstream.client.api.StreamClient;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Elastic log dimension stream manager.
 */
public class ElasticLogStreamManager {
    private final Map<String, LazyStream> streamMap = new ConcurrentHashMap<>();
    private final StreamClient streamClient;
    private final int replicaCount;
    /**
     * inner listener for created LazyStream
     */
    private final LazyStreamEventListener innerListener = new LazyStreamEventListener();
    /**
     * outer register listener
     */
    private ElasticEventListener outerListener;

    public ElasticLogStreamManager(Map<String, Long> streams, StreamClient streamClient, int replicaCount) {
        this.streamClient = streamClient;
        this.replicaCount = replicaCount;
        streams.forEach((name, streamId) -> {
            try {
                streamMap.put(name, new LazyStream(name, streamId, streamClient, replicaCount));
            } catch (Exception e) {
                // TODO: handle exception
                throw new RuntimeException(e);
            }
        });
    }

    public LazyStream getStream(String name) {
        return streamMap.computeIfAbsent(name, key -> {
            try {
                LazyStream lazyStream = new LazyStream(name, LazyStream.NOOP_STREAM_ID, streamClient, replicaCount);
                lazyStream.setListener(innerListener);
                return lazyStream;
            } catch (Exception e) {
                // TODO: handle exception
                return null;
            }
        });
    }

    public Map<String, Stream> streams() {
        return Collections.unmodifiableMap(streamMap);
    }

    public void setListener(ElasticEventListener listener) {
        this.outerListener = listener;
    }

    public void close() {
        // TODO: close stream recycle resource.
    }

    class LazyStreamEventListener implements ElasticEventListener {
        @Override
        public void onEvent(long streamId, ElasticMetaEvent event) {
            Optional.ofNullable(outerListener).ifPresent(listener -> listener.onEvent(streamId, event));
        }
    }
}
