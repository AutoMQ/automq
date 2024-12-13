/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.shell.util;

import com.automq.stream.s3.ByteBufAlloc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBuf;

public class Utils {

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
