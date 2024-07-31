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

package com.automq.stream.s3.trace.aop;

import org.aspectj.lang.annotation.Aspect;

@Aspect
public class S3StreamTraceAspect {

    // Commented out because it's costly to trace
    //
    // @Pointcut("@annotation(withSpan)")
    // public void trace(WithSpan withSpan) {
    // }
    //
    // @Around(value = "trace(withSpan) && execution(* com.automq.stream..*(..))", argNames = "joinPoint,withSpan")
    // public Object createSpan(ProceedingJoinPoint joinPoint, WithSpan withSpan) throws Throwable {
    //     Object[] args = joinPoint.getArgs();
    //     if (args.length > 0 && args[0] instanceof TraceContext) {
    //         TraceContext context = (TraceContext) args[0];
    //         return TraceUtils.trace(context, joinPoint, withSpan);
    //     }
    //
    //     return joinPoint.proceed();
    // }
}
