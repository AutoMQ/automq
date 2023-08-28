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

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class BrokerS3WALMetadataImage {

    public static final BrokerS3WALMetadataImage EMPTY = new BrokerS3WALMetadataImage(-1, List.of());
    private final int brokerId;
    private final List<S3WALObject> s3WalObjects;

    public BrokerS3WALMetadataImage(int brokerId, List<S3WALObject> s3WalObjects) {
        this.brokerId = brokerId;
        this.s3WalObjects = s3WalObjects;
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
        return brokerId == that.brokerId && Objects.equals(s3WalObjects, that.s3WalObjects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, s3WalObjects);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(new ApiMessageAndVersion(new BrokerWALMetadataRecord()
            .setBrokerId(brokerId), (short) 0));
        s3WalObjects.forEach(walObject -> writer.write(walObject.toRecord()));
    }

    public List<S3WALObject> getWalObjects() {
        return s3WalObjects;
    }

    public int getBrokerId() {
        return brokerId;
    }
}
