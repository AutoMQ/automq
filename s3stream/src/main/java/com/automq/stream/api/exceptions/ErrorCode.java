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

package com.automq.stream.api.exceptions;

public class ErrorCode {

    public static final short UNEXPECTED = 99;

    public static final short STREAM_ALREADY_CLOSED = 1;
    public static final short STREAM_NOT_EXIST = 2;
    public static final short EXPIRED_STREAM_EPOCH = 3;
    public static final short STREAM_NOT_CLOSED = 4;

    public static final short OFFSET_OUT_OF_RANGE_BOUNDS = 10;
    public static final short FAST_READ_FAIL_FAST = 11;

}
