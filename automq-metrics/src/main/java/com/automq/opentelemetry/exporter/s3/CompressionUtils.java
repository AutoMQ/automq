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

package com.automq.opentelemetry.exporter.s3;

import com.automq.stream.s3.ByteBufAlloc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBuf;

/**
 * Utility class for data compression and decompression.
 */
public class CompressionUtils {

    /**
     * Compress a ByteBuf using GZIP.
     *
     * @param input The input ByteBuf to compress.
     * @return A new ByteBuf containing the compressed data.
     * @throws IOException If an I/O error occurs during compression.
     */
    public static ByteBuf compress(ByteBuf input) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);

        byte[] buffer = new byte[input.readableBytes()];
        input.readBytes(buffer);
        gzipOutputStream.write(buffer);
        gzipOutputStream.close();

        ByteBuf compressed = ByteBufAlloc.byteBuffer(byteArrayOutputStream.size());
        compressed.writeBytes(byteArrayOutputStream.toByteArray());
        return compressed;
    }

    /**
     * Decompress a GZIP-compressed ByteBuf.
     *
     * @param input The compressed ByteBuf to decompress.
     * @return A new ByteBuf containing the decompressed data.
     * @throws IOException If an I/O error occurs during decompression.
     */
    public static ByteBuf decompress(ByteBuf input) throws IOException {
        byte[] compressedData = new byte[input.readableBytes()];
        input.readBytes(compressedData);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedData);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, bytesRead);
        }

        gzipInputStream.close();
        byteArrayOutputStream.close();

        byte[] uncompressedData = byteArrayOutputStream.toByteArray();
        ByteBuf output = ByteBufAlloc.byteBuffer(uncompressedData.length);
        output.writeBytes(uncompressedData);
        return output;
    }
}
