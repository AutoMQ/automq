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

public final class S3StreamsMetadataImage {

    public static final S3StreamsMetadataImage EMPTY =
        new S3StreamsMetadataImage(Collections.emptyMap(), Collections.emptyMap());

    private final Map<Long/*streamId*/, S3StreamMetadataImage> streamsMetadata;

    private final Map<Integer/*brokerId*/, BrokerS3WALMetadataImage> brokerWALMetadata;

    public S3StreamsMetadataImage(
        Map<Long, S3StreamMetadataImage> streamsMetadata,
        Map<Integer, BrokerS3WALMetadataImage> brokerWALMetadata) {
        this.streamsMetadata = streamsMetadata;
        this.brokerWALMetadata = brokerWALMetadata;
    }


    boolean isEmpty() {
        return this.brokerWALMetadata.isEmpty() && this.streamsMetadata.isEmpty();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        streamsMetadata.values().forEach(image -> image.write(writer, options));
        brokerWALMetadata.values().forEach(image -> image.write(writer, options));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof S3StreamsMetadataImage)) return false;
        S3StreamsMetadataImage other = (S3StreamsMetadataImage) obj;
        return this.streamsMetadata.equals(other.streamsMetadata)
            && this.brokerWALMetadata.equals(other.brokerWALMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamsMetadata, brokerWALMetadata);
    }

    public Map<Integer, BrokerS3WALMetadataImage> getBrokerWALMetadata() {
        return brokerWALMetadata;
    }

    public Map<Long, S3StreamMetadataImage> getStreamsMetadata() {
        return streamsMetadata;
    }
}
