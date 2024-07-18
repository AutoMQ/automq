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

import com.automq.stream.s3.trace.context.TraceContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

public class AppendContext extends TraceContext {
    public static final AppendContext DEFAULT = new AppendContext();

    public AppendContext() {
        super(false, null, null);
    }

    public AppendContext(TraceContext context) {
        super(context);
    }

    public AppendContext(boolean isTraceEnabled, Tracer tracer, Context currentContext) {
        super(isTraceEnabled, tracer, currentContext);
    }
}
