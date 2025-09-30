package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static com.automq.opentelemetry.exporter.remotewrite.auth.AzureCloudConst.AZURE_CHINA;
import static com.automq.opentelemetry.exporter.remotewrite.auth.AzureCloudConst.AZURE_GOVERNMENT;
import static com.automq.opentelemetry.exporter.remotewrite.auth.AzureCloudConst.AZURE_PUBLIC;

public class AzureADAuth implements RemoteWriteAuth {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureADAuth.class);
    private final String cloudAudience;
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;
    private final Map<String, String> headers;

    public AzureADAuth(String cloud, String clientId, String clientSecret, String tenantId) {
        this(cloud, clientId, clientSecret, tenantId, Collections.emptyMap());
    }

    public AzureADAuth(String cloud, String clientId, String clientSecret, String tenantId, Map<String, String> headers) {
        this.cloudAudience = toCloudAudience(cloud, StringUtils.isBlank(clientSecret));
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tenantId = tenantId;
        this.headers = headers;
    }

    public String toCloudAudience(String cloud, boolean isManagedIdentity) {
        return switch (cloud.toLowerCase()) {
            case AZURE_CHINA -> isManagedIdentity ? AzureCloudConst.azureChinaAudienceManagedIdentity : AzureCloudConst.azureChinaAudienceClientSecret;
            case AZURE_PUBLIC -> isManagedIdentity ? AzureCloudConst.azurePublicAudienceManagedIdentity : AzureCloudConst.azurePublicAudienceClientSecret;
            case AZURE_GOVERNMENT -> isManagedIdentity ? AzureCloudConst.azureGovernmentAudienceManagedIdentity : AzureCloudConst.azureGovernmentAudienceClientSecret;
            default -> throw new IllegalArgumentException("Unknown Azure cloud: " + cloud);
        };
    }

    public String getCloudAudience() {
        return cloudAudience;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public boolean validate() {
        if (clientId == null || clientId.isEmpty()) {
            LOGGER.error("Client ID is required for Azure AD authentication.");
            return false;
        }
        if (cloudAudience == null || cloudAudience.isEmpty()) {
            LOGGER.error("Cloud audience is required for Azure AD authentication.");
            return false;
        }
        return true;
    }

    @Override
    public AuthType authType() {
        return AuthType.AZURE_AD;
    }

    @Override
    public Interceptor createInterceptor() {
        return new AzureADInterceptor(cloudAudience, clientId, clientSecret, tenantId, headers);
    }
}
