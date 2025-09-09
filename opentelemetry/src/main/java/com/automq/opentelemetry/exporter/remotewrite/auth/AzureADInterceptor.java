package com.automq.opentelemetry.exporter.remotewrite.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AzureADInterceptor implements Interceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureADInterceptor.class);
    private final ScheduledExecutorService scheduledExecutorService;
    private final String cloudAudience;
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;
    private final Map<String, String> headers;
    private final TokenCredential credential;
    private volatile String token;

    public AzureADInterceptor(String cloudAudience, String clientId, String clientSecret, String tenantId) {
        this(cloudAudience, clientId, clientSecret, tenantId, Collections.emptyMap());
    }

    public AzureADInterceptor(String cloudAudience, String clientId, String clientSecret, String tenantId, Map<String, String> headers) {
        this.cloudAudience = cloudAudience;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tenantId = tenantId;
        this.headers = headers;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("azure-ad-token-refresh"));
        this.credential = getTokenCredential();
        refreshToken();
    }

    private void refreshToken() {
        // Refresh token logic
        AccessToken accessToken = this.credential.getTokenSync(new TokenRequestContext().addScopes(cloudAudience));
        long now = System.currentTimeMillis();
        this.token = accessToken.getToken();
        long localExpireTimeMs = accessToken.getExpiresAt().atZoneSameInstant(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long refreshDelay = (localExpireTimeMs - now) / 2;
        LOGGER.info("Azure AD token refreshed at {}, expires at {}, refresh delay: {}ms.", now, accessToken.getExpiresAt(), refreshDelay);
        this.scheduledExecutorService.schedule(this::refreshToken, refreshDelay, TimeUnit.MILLISECONDS);
    }

    private TokenCredential getTokenCredential() {
        if (clientSecret != null && !clientSecret.isEmpty()) {
            return new ClientSecretCredentialBuilder()
                .clientSecret(clientSecret)
                .clientId(clientId)
                .tenantId(tenantId).build();
        } else {
            return new DefaultAzureCredentialBuilder()
                .managedIdentityClientId(clientId)
                .build();
        }
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request.Builder builder = chain.request()
            .newBuilder()
            .header(AuthUtils.AUTH_HEADER, "Bearer " + token);
        headers.forEach(builder::header);
        return chain.proceed(builder.build());
    }
}
