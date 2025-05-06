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

package kafka.automq.table.transformer;

import java.nio.ByteBuffer;

public class FieldMetric {

    public static int count(String value) {
        if (value == null) {
            return 0;
        }
        return Math.max((value.length() + 23) / 24, 1);
    }

    public static int count(ByteBuffer value) {
        if (value == null) {
            return 0;
        }
        return Math.max((value.remaining() + 31) / 32, 1);
    }

    public static int count(byte[] value) {
        if (value == null) {
            return 0;
        }
        return Math.max((value.length + 31) / 32, 1);
    }

}
