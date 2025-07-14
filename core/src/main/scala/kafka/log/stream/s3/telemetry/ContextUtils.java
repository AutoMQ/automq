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

package kafka.log.stream.s3.telemetry;

import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.trace.context.TraceContext;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;

public class ContextUtils {
    public static FetchContext creaetFetchContext() {
        return new FetchContext(createTraceContext());
    }

    public static AppendContext createAppendContext() {
        return new AppendContext(createTraceContext());
    }

    public static TraceContext createTraceContext() {
        OpenTelemetrySdk openTelemetrySdk = TelemetryManager.getOpenTelemetrySdk();
        boolean isTraceEnabled = openTelemetrySdk != null && TelemetryManager.isTraceEnable();
        Tracer tracer = null;
        if (isTraceEnabled) {
            tracer = openTelemetrySdk.getTracer(TelemetryConstants.TELEMETRY_SCOPE_NAME);
        }
        return new TraceContext(isTraceEnabled, tracer, Context.current());
    }

}
