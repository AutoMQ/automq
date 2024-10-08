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
