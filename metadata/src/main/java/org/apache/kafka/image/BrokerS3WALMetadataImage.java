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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class BrokerS3WALMetadataImage {

    public static final BrokerS3WALMetadataImage EMPTY = new BrokerS3WALMetadataImage(S3StreamConstant.INVALID_BROKER_ID,
        S3StreamConstant.INVALID_BROKER_EPOCH, Collections.emptyMap());
    private final int brokerId;
    private final long brokerEpoch;
    private final Map<Long/*objectId*/, S3WALObject> s3WalObjects;
    private final SortedMap<Long/*orderId*/, S3WALObject> orderIndex;

    public BrokerS3WALMetadataImage(int brokerId, long brokerEpoch, Map<Long, S3WALObject> walObjects) {
        this.brokerId = brokerId;
        this.brokerEpoch = brokerEpoch;
        this.s3WalObjects = new HashMap<>(walObjects);
        // build order index
        if (s3WalObjects.isEmpty()) {
            this.orderIndex = Collections.emptySortedMap();
        } else {
            this.orderIndex = new TreeMap<>();
            s3WalObjects.values().forEach(s3WALObject -> orderIndex.put(s3WALObject.orderId(), s3WALObject));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BrokerS3WALMetadataImage that = (BrokerS3WALMetadataImage) o;
        return brokerId == that.brokerId && brokerEpoch == that.brokerEpoch && Objects.equals(s3WalObjects, that.s3WalObjects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, brokerEpoch, s3WalObjects);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(new ApiMessageAndVersion(new BrokerWALMetadataRecord()
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch), (short) 0));
        s3WalObjects.values().forEach(wal -> {
            writer.write(wal.toRecord());
        });
    }

    public Map<Long, S3WALObject> getWalObjects() {
        return s3WalObjects;
    }

    public SortedMap<Long, S3WALObject> getOrderIndex() {
        return orderIndex;
    }

    public List<S3WALObject> orderList() {
        return orderIndex.values().stream().collect(Collectors.toList());
    }

    public int getBrokerId() {
        return brokerId;
    }

    public long getBrokerEpoch() {
        return brokerEpoch;
    }

    @Override
    public String toString() {
        return "BrokerS3WALMetadataImage{" +
            "brokerId=" + brokerId +
            ", brokerEpoch=" + brokerEpoch +
            ", s3WalObjects=" + s3WalObjects +
            '}';
    }
}
