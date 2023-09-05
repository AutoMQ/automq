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

import java.util.Iterator;
import java.util.Objects;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.SortedWALObjects;
import org.apache.kafka.metadata.stream.SortedWALObjectsList;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class BrokerS3WALMetadataImage {

    public static final BrokerS3WALMetadataImage EMPTY = new BrokerS3WALMetadataImage(S3StreamConstant.INVALID_BROKER_ID, new SortedWALObjectsList());
    private final int brokerId;
    private final SortedWALObjects s3WalObjects;

    public BrokerS3WALMetadataImage(int brokerId, SortedWALObjects sourceWALObjects) {
        this.brokerId = brokerId;
        this.s3WalObjects = new SortedWALObjectsList(sourceWALObjects);
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
        Iterator<S3WALObject> iterator = s3WalObjects.iterator();
        while (iterator.hasNext()) {
            S3WALObject s3WALObject = iterator.next();
            writer.write(s3WALObject.toRecord());
        }
    }

    public SortedWALObjects getWalObjects() {
        return s3WalObjects;
    }

    public int getBrokerId() {
        return brokerId;
    }

    @Override
    public String toString() {
        return "BrokerS3WALMetadataImage{" +
            "brokerId=" + brokerId +
            ", s3WalObjects=" + s3WalObjects +
            '}';
    }
}
