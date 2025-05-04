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

package com.automq.stream.s3.context;

import com.automq.stream.api.ReadOptions;
import com.automq.stream.s3.trace.context.TraceContext;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

public class FetchContext extends TraceContext {
    public static final FetchContext DEFAULT = new FetchContext();
    private ReadOptions readOptions = ReadOptions.DEFAULT;

    public FetchContext() {
        super(false, null, null);
    }

    public FetchContext(TraceContext context) {
        super(context);
    }

    public FetchContext(boolean isTraceEnabled, Tracer tracer, Context currentContext) {
        super(isTraceEnabled, tracer, currentContext);
    }

    public ReadOptions readOptions() {
        return readOptions;
    }

    public void setReadOptions(ReadOptions readOptions) {
        this.readOptions = readOptions;
    }
}
