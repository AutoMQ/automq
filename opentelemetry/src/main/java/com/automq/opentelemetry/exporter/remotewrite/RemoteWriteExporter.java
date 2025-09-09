package com.automq.opentelemetry.exporter.remotewrite;

import com.automq.opentelemetry.telemetry.RemoteWrite;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RemoteWriteExporter implements MetricExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteExporter.class);
    private final OkHttpClient client;
    private final String endpoint;
    private final int maxBatchSize;

    public RemoteWriteExporter(RemoteWriteURI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("Remote write URI is required");
        }
        this.endpoint = uri.endpoint();
        this.maxBatchSize = uri.maxBatchSize();
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
        if (uri.auth() != null) {
            try {
                clientBuilder.addNetworkInterceptor(uri.auth().createInterceptor());
            } catch (IllegalArgumentException e) {
                LOGGER.error("Failed to create remote write authenticator", e);
            }
        }
        if (uri.insecureSkipVerify()) {
            skipTLSVerification(clientBuilder);
        }
        this.client = clientBuilder.build();
    }

    private void skipTLSVerification(OkHttpClient.Builder builder) {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {

                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[]{};
                    }
                }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create a ssl socket factory with the all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
            // Trust all hostnames
            builder.hostnameVerifier((hostname, session) -> true);
            LOGGER.warn("TLS verification is disabled");
        } catch (Exception e) {
            LOGGER.error("Failed to skip TLS verification", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableResultCode export(@NotNull Collection<MetricData> collection) {
        LOGGER.info("Exporting remote writes to remote write endpoint");
        LOGGER.info("Endpoint: {}", endpoint);
        LOGGER.info("collection size: {}", collection.size());
        LOGGER.info("collection: {}", collection);
        RemoteWriteRequestMarshaller marshaller = new RemoteWriteRequestMarshaller();
        Collection<RemoteWrite.TimeSeries> timeSeries;
        CompletableResultCode code;
        try {
            timeSeries = marshaller.fromMetrics(collection);
            code = sendBatchRequests(timeSeries);
        } catch (Exception e) {
            LOGGER.error("Failed to export metrics", e);
            return CompletableResultCode.ofFailure();
        }
        return code;
    }

    private CompletableResultCode sendBatchRequests(Collection<RemoteWrite.TimeSeries> timeSeries) {
        LOGGER.info("Sending batch requests");
        LOGGER.info("timeSeries: {}", timeSeries);
        long currentSize = 0;
        List<CompletableResultCode> codes = new ArrayList<>();
        RemoteWrite.WriteRequest.Builder requestBuilder = RemoteWrite.WriteRequest.newBuilder();
        for (RemoteWrite.TimeSeries ts : timeSeries) {
            long batchSize = ts.getSerializedSize();
            if (currentSize + batchSize > this.maxBatchSize) {
                CompletableResultCode code = new CompletableResultCode();
                codes.add(code);
                try {
                    sendRequest(requestBuilder.build(), code);
                } catch (IOException e) {
                    LOGGER.error("Failed to send remote write request", e);
                    code.fail();
                }
                requestBuilder = RemoteWrite.WriteRequest.newBuilder();
                currentSize = 0;
            } else {
                requestBuilder.addTimeseries(ts);
                currentSize += batchSize;
            }
        }
        if (currentSize > 0) {
            CompletableResultCode code = new CompletableResultCode();
            codes.add(code);
            try {
                sendRequest(requestBuilder.build(), code);
            } catch (IOException e) {
                LOGGER.error("Failed to send remote write request", e);
                code.fail();
            }
        }
        return CompletableResultCode.ofAll(codes);
    }

    private void sendRequest(RemoteWrite.WriteRequest writeRequest, CompletableResultCode code) throws IOException {
        LOGGER.info("Sending remote write request");
        byte[] compressed = Snappy.compress(writeRequest.toByteArray());
        MediaType mediaType = MediaType.parse("application/x-protobuf");
        RequestBody body = RequestBody.create(compressed, mediaType);
        Request request = new Request.Builder()
            .url(endpoint)
            .addHeader("Content-Encoding", "snappy")
            .addHeader("User-Agent", "automq-exporter/1.1.0")
            .addHeader("X-Prometheus-Remote-Write-Version", "0.1.0")
            .post(body)
            .build();
        
        LOGGER.info("Sending remote write request:{},{}", request.body(), request.url());
        client.newCall(request).enqueue(new Callback() {

            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                LOGGER.error("Failed to send remote write request", e);
                code.fail();
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                try (ResponseBody body = response.body()) {
                    LOGGER.info("Received remote write response:{}", response.body().string());
                    if (response.code() >= 200 && response.code() <= 299) {
                        code.succeed();
                        return;
                    }
                    LOGGER.error("Remote write request not success, code: {}, resp: {}", response.code(),
                        body == null ? "" : body.string());
                    code.fail();
                }
            }
        });
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(@NotNull InstrumentType type) {
        return AggregationTemporality.CUMULATIVE;
    }
}
