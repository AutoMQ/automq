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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * 是一个从零开始无限增长的 WAL，实际实现会使用块设备，每次写入采用块对齐方式。Record 之间非连续存储。
 * 如果上一次是优雅关闭，重新启动后，read 返回的数据为空集合。
 */
public interface WAL {
    /**
     * 启动线程，加载元数据
     */
    WAL start() throws IOException;

    /**
     * 关闭线程，保存元数据，其中包含 trim offset。
     */
    void shutdownGracefully();

    /**
     * trim 不及时，会抛异常。
     * 滑动窗口无法扩容，也会抛异常。
     *
     * @throws OverCapacityException
     */
    AppendResult append(ByteBuffer record, //
                        int crc // 如果 crc == 0，表示需要重新计算 crc
    ) throws OverCapacityException;

    Iterator<RecoverResult> recover();

    Iterator<RecoverResult> recover(long startOffset);

    /**
     * 抹除所有小于 offset 的数据。
     * >= offset 的数据仍然可以读取。
     */
    void trim(long offset);

    interface AppendResult {

        // Record body 预分配的存储起始位置
        long recordBodyOffset();
        // Record body 的长度（不包含任何元数据长度）

        int recordBodyCRC();

        int length();

        CompletableFuture<CallbackResult> future();

        interface CallbackResult {
            // 这个 Offset 之前的数据已经落盘
            long flushedOffset();

            AppendResult appendResult();
        }
    }

    interface RecoverResult {
        ByteBuffer record();

        AppendResult appendResult();
    }

    class OverCapacityException extends Exception {
        private final long flushedOffset;

        public OverCapacityException(String message, long flushedOffset) {
            super(message);
            this.flushedOffset = flushedOffset;
        }

        public long flushedOffset() {
            return flushedOffset;
        }
    }
}

