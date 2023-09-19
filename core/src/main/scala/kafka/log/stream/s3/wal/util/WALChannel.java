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

package kafka.log.stream.s3.wal.util;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 对访问 WAL 读写进行抽象，如果底层是块设备，则实现为对块设备以 O_DIRECT 方式读写。
 * 如果底层是文件系统，则实现为对文件系统的读写，每次写同步调用 fsync，保证数据落盘。
 */
public interface WALChannel {
    void open() throws IOException;

    void close();

    long capacity();

    /**
     * 将 src 中的数据写入到 position 位置，只有全部写入完成，才返回成功。否则抛异常
     */
    void write(ByteBuffer src, long position) throws IOException;

    int read(ByteBuffer dst, long position) throws IOException;

    class WALChannelBuilder {
        public static WALChannel build(String path, long maxCapacity) {
            if (path.startsWith("/dev/")) {
                return new WALBlockDeviceChannel(path, maxCapacity);
            } else {
                return new WALFileChannel(path, maxCapacity);
            }
        }
    }
}
