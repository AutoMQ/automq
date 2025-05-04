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
