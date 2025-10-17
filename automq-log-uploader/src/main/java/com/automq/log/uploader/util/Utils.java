/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.log.uploader.util;

import com.automq.stream.s3.ByteBufAlloc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBuf;

public class Utils {

    private Utils() {
    }

    public static ByteBuf compress(ByteBuf input) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            byte[] buffer = new byte[input.readableBytes()];
            input.readBytes(buffer);
            gzipOutputStream.write(buffer);
        }

        ByteBuf compressed = ByteBufAlloc.byteBuffer(byteArrayOutputStream.size());
        compressed.writeBytes(byteArrayOutputStream.toByteArray());
        return compressed;
    }

    public static ByteBuf decompress(ByteBuf input) throws IOException {
        byte[] compressedData = new byte[input.readableBytes()];
        input.readBytes(compressedData);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedData);

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }

            byte[] uncompressedData = byteArrayOutputStream.toByteArray();
            ByteBuf output = ByteBufAlloc.byteBuffer(uncompressedData.length);
            output.writeBytes(uncompressedData);
            return output;
        }
    }
}
