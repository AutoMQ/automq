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

package com.automq.stream.s3.wal;


import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public interface WriteAheadLog {

    WriteAheadLog start() throws IOException;

    void shutdownGracefully();

    /**
     * Append data to log, note append may be out of order.
     * ex. when sequence append R1 R2 , R2 maybe complete before R1.
     *
     * @return The data position will be written.
     */
    AppendResult append(ByteBuf data, int crc) throws OverCapacityException;

    default AppendResult append(ByteBuf data) throws OverCapacityException {
        return append(data, 0);
    }

    Iterator<RecoverResult> recover();

    /**
     * Reset all data in log.
     * Equivalent to trim to the end of the log.
     *
     * @return future complete when reset done.
     */
    CompletableFuture<Void> reset();

    /**
     * Trim data <= offset in log.
     *
     * @param offset inclusive trim offset.
     * @return future complete when trim done.
     */
    CompletableFuture<Void> trim(long offset);

    interface AppendResult {
        // The pre-allocated starting offset of the record
        long recordOffset();

        CompletableFuture<CallbackResult> future();

        interface CallbackResult {
            // The record before this offset has been flushed to disk
            long flushedOffset();
        }
    }

    interface RecoverResult {
        ByteBuffer record();

        /**
         * @see AppendResult#recordOffset()
         */
        long recordOffset();
    }

    class OverCapacityException extends Exception {
        public OverCapacityException(String message) {
            super(message);
        }
    }
}
