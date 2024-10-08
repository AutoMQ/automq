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

package com.automq.stream.s3.trace.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

/**
 * Trace context that holds the current trace context. This class is not thread safe and should be copied before
 * asynchronous usage.
 */
@NotThreadSafe
public class TraceContext {
    public static final TraceContext DEFAULT = new TraceContext(false, null, null);
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceContext.class);
    private final boolean isTraceEnabled;
    private final Tracer tracer;
    private Context currContext;

    public TraceContext(boolean isTraceEnabled, Tracer tracer, Context currContext) {
        this.isTraceEnabled = isTraceEnabled;
        if (isTraceEnabled && tracer == null) {
            this.tracer = GlobalOpenTelemetry.getTracer("s3stream");
        } else {
            this.tracer = tracer;
        }
        if (isTraceEnabled && currContext == null) {
            this.currContext = Context.current();
        } else {
            this.currContext = currContext;
        }
    }

    public TraceContext(TraceContext traceContext) {
        this(traceContext.isTraceEnabled, traceContext.tracer, traceContext.currContext);
    }

    public boolean isTraceDisabled() {
        return !isTraceEnabled;
    }

    public Tracer tracer() {
        return tracer;
    }

    public Context currentContext() {
        return currContext;
    }

    public Scope attachContext(Context contextToAttach) {
        return new Scope(contextToAttach);
    }

    public class Scope implements AutoCloseable {
        private final Context prevContext;
        private final Span span;

        private Scope(Context contextToAttach) {
            this.prevContext = currContext;
            this.span = Span.fromContext(contextToAttach);
            currContext = contextToAttach;
        }

        public Span getSpan() {
            return span;
        }

        @Override
        public void close() {
            currContext = prevContext;
        }
    }

}
