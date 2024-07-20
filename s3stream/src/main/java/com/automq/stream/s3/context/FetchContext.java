/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
