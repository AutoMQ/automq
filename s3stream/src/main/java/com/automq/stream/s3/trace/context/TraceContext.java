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

package com.automq.stream.s3.trace.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

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
    private static volatile Supplier<Tracer> tracerSupplier;

    private final boolean isTraceEnabled;
    private final Tracer tracer;
    private Context currContext;

    public static void setTracerSupplier(Supplier<Tracer> supplier) {
        tracerSupplier = supplier;
    }

    public TraceContext(boolean isTraceEnabled, Tracer tracer, Context currContext) {
        this.isTraceEnabled = isTraceEnabled;
        if (isTraceEnabled && tracer == null) {
            if (tracerSupplier != null) {
                this.tracer = tracerSupplier.get();
            } else {
                this.tracer = GlobalOpenTelemetry.getTracer("s3stream");
            }
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
