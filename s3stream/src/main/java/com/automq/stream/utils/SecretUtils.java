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

package com.automq.stream.utils;

public class SecretUtils {
    public static String mask(String str) {
        if (str == null) {
            return str;
        }
        int prefixLength = str.length() / 4;
        int suffixLength = str.length() / 4;
        if (str.length() <= prefixLength + suffixLength) {
            return str;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(str, 0, prefixLength);
        int maskLength = str.length() - prefixLength - suffixLength;
        sb.append("*".repeat(Math.max(0, maskLength)));
        sb.append(str.substring(str.length() - suffixLength));
        return sb.toString();
    }
}
