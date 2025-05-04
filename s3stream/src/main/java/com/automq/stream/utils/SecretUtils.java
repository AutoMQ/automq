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
