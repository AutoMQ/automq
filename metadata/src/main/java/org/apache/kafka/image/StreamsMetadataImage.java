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

package org.apache.kafka.image;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

public final class StreamsMetadataImage {

    public static final StreamsMetadataImage EMPTY =
        new StreamsMetadataImage(Collections.emptyMap(), Collections.emptyMap());

    private final Map<Long/*streamId*/, StreamMetadataImage> streamsMetadata;

    private final Map<Integer/*brokerId*/, BrokerStreamMetadataImage> brokerStreamsMetadata;

    public StreamsMetadataImage(
        Map<Long, StreamMetadataImage> streamsMetadata,
        Map<Integer, BrokerStreamMetadataImage> brokerStreamsMetadata) {
        this.streamsMetadata = streamsMetadata;
        this.brokerStreamsMetadata = brokerStreamsMetadata;
    }


    boolean isEmpty() {
        return this.brokerStreamsMetadata.isEmpty() && this.streamsMetadata.isEmpty();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        streamsMetadata.values().forEach(image -> image.write(writer, options));
        brokerStreamsMetadata.values().forEach(image -> image.write(writer, options));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StreamsMetadataImage)) return false;
        StreamsMetadataImage other = (StreamsMetadataImage) obj;
        return this.streamsMetadata.equals(other.streamsMetadata)
            && this.brokerStreamsMetadata.equals(other.brokerStreamsMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamsMetadata, brokerStreamsMetadata);
    }

    public Map<Integer, BrokerStreamMetadataImage> getBrokerStreamsMetadata() {
        return brokerStreamsMetadata;
    }

    public Map<Long, StreamMetadataImage> getStreamsMetadata() {
        return streamsMetadata;
    }
}
