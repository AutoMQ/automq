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
package org.apache.kafka.common.record;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public class DefaultRecordsSend extends RecordsSend<Records> {
    public DefaultRecordsSend(String destination, Records records) {
        this(destination, records, records.sizeInBytes());
    }

    public DefaultRecordsSend(String destination, Records records, int maxBytesToWrite) {
        super(destination, records, maxBytesToWrite);
    }

    @Override
    protected long writeTo(GatheringByteChannel channel, long previouslyWritten, int remaining) throws IOException {
        return records().writeTo(channel, previouslyWritten, remaining);
    }
}
