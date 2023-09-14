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

package com.automq.rocketmq.stream.api;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Record batch.
 */
public interface RecordBatch {

    /**
     * Get payload record count.
     *
     * @return record count.
     */
    int count();

    /**
     * Get min timestamp of records.
     *
     * @return min timestamp of records.
     */
    long baseTimestamp();

    /**
     * Get record batch extension properties.
     *
     * @return batch extension properties.
     */
    Map<String, String> properties();

    /**
     * Get raw payload.
     *
     * @return raw payload.
     */
    ByteBuffer rawPayload();
}
