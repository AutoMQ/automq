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

package com.automq.stream.s3.trace;

import com.automq.stream.s3.trace.context.TraceContext;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;

public class TraceUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceUtils.class);
    private static final SpanAttributesExtractor EXTRACTOR = SpanAttributesExtractor.create();

    public static Object trace(TraceContext context, ProceedingJoinPoint joinPoint,
        WithSpan withSpan) throws Throwable {
        if (context.isTraceDisabled()) {
            return joinPoint.proceed();
        }

        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();

        String className = method.getDeclaringClass().getSimpleName();
        String spanName = withSpan.value().isEmpty() ? className + "::" + method.getName() : withSpan.value();

        TraceContext.Scope scope = createAndStartSpan(context, spanName);
        if (scope == null) {
            return joinPoint.proceed();
        }
        Span span = scope.getSpan();
        Attributes attributes = EXTRACTOR.extract(method, signature.getParameterNames(), args);
        span.setAllAttributes(attributes);

        try {
            if (method.getReturnType() == CompletableFuture.class) {
                return doTraceWhenReturnCompletableFuture(scope, joinPoint);
            } else {
                return doTraceWhenReturnObject(scope, joinPoint);
            }
        } catch (Throwable t) {
            endSpan(scope, t);
            throw t;
        }
    }

    public static <T> T runWithSpanSync(TraceContext context, Attributes attributes, String spanName,
        Callable<T> callable) throws Throwable {
        TraceContext.Scope scope = createAndStartSpan(context, spanName);
        if (scope == null) {
            return callable.call();
        }
        scope.getSpan().setAllAttributes(attributes);
        try (scope) {
            T ret = callable.call();
            endSpan(scope, null);
            return ret;
        } catch (Throwable t) {
            endSpan(scope, t);
            throw t;
        }
    }

    public static <T> CompletableFuture<T> runWithSpanAsync(TraceContext context, Attributes attributes,
        String spanName,
        Callable<CompletableFuture<T>> callable) throws Throwable {
        TraceContext.Scope scope = createAndStartSpan(context, spanName);
        if (scope == null) {
            return callable.call();
        }
        scope.getSpan().setAllAttributes(attributes);
        try (scope) {
            CompletableFuture<T> cf = callable.call();
            cf.whenComplete((nil, ex) -> endSpan(scope, ex));
            return cf;
        } catch (Throwable t) {
            endSpan(scope, t);
            throw t;
        }
    }

    public static TraceContext.Scope createAndStartSpan(TraceContext context, String name) {
        if (context.isTraceDisabled()) {
            return null;
        }
        Tracer tracer = context.tracer();
        Context parentContext = context.currentContext();
        Span span = tracer.spanBuilder(name)
            .setParent(parentContext)
            .startSpan();

        return context.attachContext(parentContext.with(span));
    }

    public static void endSpan(TraceContext.Scope scope, Throwable t) {
        if (scope == null) {
            return;
        }
        if (t != null) {
            scope.getSpan().recordException(t);
            scope.getSpan().setStatus(StatusCode.ERROR, t.getMessage());
        } else {
            scope.getSpan().setStatus(StatusCode.OK);
        }
        scope.getSpan().end();
        scope.close();
    }

    private static CompletableFuture<?> doTraceWhenReturnCompletableFuture(TraceContext.Scope scope,
        ProceedingJoinPoint joinPoint) throws Throwable {
        CompletableFuture<?> future = (CompletableFuture<?>) joinPoint.proceed();
        return future.whenComplete((r, t) -> endSpan(scope, t));
    }

    private static Object doTraceWhenReturnObject(TraceContext.Scope scope,
        ProceedingJoinPoint joinPoint) throws Throwable {
        Object result = joinPoint.proceed();
        endSpan(scope, null);
        return result;
    }

}
