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

package kafka.log.s3.wal;


import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface WriteAheadLog {

    /**
     * Get log start position.
     * @return start position.
     */
    long startPosition();

    /**
     * Get log end position.
     * @return end position.
     */
    long endPosition();

    /**
     * Read data from log.
     * @return list of {@link WalRecord}.
     */
    List<WalRecord> read();

    /**
     * Append data to log, note append may be out of order.
     * ex. when sequence append R1 R2 , R2 maybe complete before R1.
     *
     * @return The data position will be written.
     */
    AppendResult append(ByteBuf data);

    /**
     * Trim log to new start position.
     *
     * @param position new start position.
     */
    void trim(long position);


    class WalRecord {
        private long endPosition;
        private ByteBuf data;

        public WalRecord(long endPosition, ByteBuf data) {
            this.endPosition = endPosition;
            this.data = data;
        }

        public long endPosition() {
            return endPosition;
        }

        public ByteBuf data() {
            return data;
        }
    }

    class AppendResult {
        public long endPosition;
        public CompletableFuture<Void> future;
    }

}
