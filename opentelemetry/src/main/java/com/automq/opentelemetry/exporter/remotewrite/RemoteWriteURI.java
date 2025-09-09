package com.automq.opentelemetry.exporter.remotewrite;

import com.automq.opentelemetry.exporter.remotewrite.auth.AuthType;
import com.automq.opentelemetry.exporter.remotewrite.auth.AwsSigV4Auth;
import com.automq.opentelemetry.exporter.remotewrite.auth.AzureADAuth;
import com.automq.opentelemetry.exporter.remotewrite.auth.BasicAuth;
import com.automq.opentelemetry.exporter.remotewrite.auth.BearerTokenAuth;
import com.automq.opentelemetry.exporter.remotewrite.auth.RemoteWriteAuth;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.utils.URIUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The remote write URI format:
 *
 * <p> 1. Basic Auth: rw://?endpoint=${endpoint}&auth=basic&username=${username}&password=${password}[&header_${headerKey}=${headerValue}]
 * <p> 2. AWS SigV4:
 * <ul>
 *     <li> With AK/SK: rw://?endpoint=${endpoint}&auth=sigv4&region=${region}&accessKey=${ak}&secretKey=${sk}[&header_${headerKey}=${headerValue}]
 *     <li> With InstanceProfile: rw://?endpoint=${endpoint}&auth=sigv4&region=${region}[&header_${headerKey}=${headerValue}]
 * </ul>
 * <p> 3. Bear Token:
 * <ul>
 *     <li> rw://?endpoint=${endpoint}&auth=bearer&token=${token}[&header_${headerKey}=${headerValue}]
 *     <li> rw://?endpoint=${endpoint}&auth=bearer&token=${token}&insecureSkipVerify=true[&header_${headerKey}=${headerValue}]
 * </ul>
 * <p> 4. Azure AD:
 *  * <ul>
 *  *     <li> With managed identity: rw://?endpoint=${endpoint}&auth=azuread&cloud=${cloud}&clientId=${clientId}[&header_${headerKey}=${headerValue}]
 *  *     <li> With oauth: rw://?endpoint=${endpoint}&auth=azuread&cloud=${cloud}&clientId=${clientId}&clientSecret=${clientSecret}&tenantId=${tenantId}[&header_${headerKey}=${headerValue}]
 *  * </ul>
 *
 * @param endpoint remote write endpoint
 * @param insecureSkipVerify whether to skip SSL verification
 * @param auth remote write auth
 * @param maxBatchSize max batch size
 */
public record RemoteWriteURI(String endpoint, boolean insecureSkipVerify, RemoteWriteAuth auth, int maxBatchSize) {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteURI.class);
    private static final int DEFAULT_MAX_BATCH_SIZE = 3000000;
    private static final String DEFAULT_AUTH_TYPE = "no_auth";
    private static final String ENDPOINT_KEY = "endpoint";
    private static final String AUTH_TYPE_KEY = "auth";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String REGION_KEY = "region";
    private static final String TOKEN_KEY = "token";
    private static final String INSECURE_SKIP_VERIFY_KEY = "insecureSkipVerify";
    private static final String MAX_BATCH_SIZE_KEY = "maxBatchSize";
    private static final String HEADER_PREFIX = "header_";
    private static final String CLOUD_KEY = "cloud";
    private static final String CLIENT_ID_KEY = "clientId";
    private static final String CLIENT_SECRET_KEY = "clientSecret";
    private static final String TENANT_ID_KEY = "tenantId";

    public RemoteWriteURI {
        if (!validate(endpoint, auth, maxBatchSize)) {
            throw new IllegalArgumentException("Illegal remote write uri");
        }
    }

    private boolean validate(String endpoint, RemoteWriteAuth auth, int maxBatchSize) {
        if (StringUtils.isBlank(endpoint)) {
            LOGGER.error("Remote write endpoint should not be empty");
            return false;
        }
        if (auth != null && !auth.validate()) {
            LOGGER.error("Remote write auth config validation failed");
            return false;
        }
        if (maxBatchSize <= 0) {
            LOGGER.error("Remote write maxBatchSize should be positive");
            return false;
        }
        return true;
    }

    public static RemoteWriteURI parse(String uriStr) {
        try {
            URI uri = new URI(uriStr);
            Map<String, List<String>> queries = URIUtils.splitQuery(uri);
            String endpoint = URIUtils.getString(queries, ENDPOINT_KEY, "");
            RemoteWriteAuth remoteWriteAuth = parseAuth(queries);
            boolean insecureSkipVerify = Boolean.parseBoolean(URIUtils.getString(queries, INSECURE_SKIP_VERIFY_KEY, "false"));
            int maxBatchSize = Integer.parseInt(URIUtils.getString(queries, MAX_BATCH_SIZE_KEY, String.valueOf(DEFAULT_MAX_BATCH_SIZE)));
            return new RemoteWriteURI(endpoint, insecureSkipVerify, remoteWriteAuth, maxBatchSize);
        } catch (URISyntaxException e) {
            LOGGER.error("Invalid remote write URI: {}", uriStr, e);
            throw new IllegalArgumentException("Invalid remote write URI " + uriStr);
        }
    }

    private static RemoteWriteAuth parseAuth(Map<String, List<String>> queries) {
        String authTypeStr = URIUtils.getString(queries, AUTH_TYPE_KEY, DEFAULT_AUTH_TYPE);
        if (authTypeStr.equals(DEFAULT_AUTH_TYPE)) {
            return null;
        }
        AuthType authType = AuthType.fromName(authTypeStr);
        if (authType == null) {
            LOGGER.error("Invalid auth type: {}, supported are: {}", authTypeStr, AuthType.getNames());
            throw new IllegalArgumentException("Invalid auth type " + authTypeStr);
        }
        Map<String, String> headers = getHeaders(queries);
        switch (authType) {
            case BASIC:
                String username = URIUtils.getString(queries, USERNAME_KEY, "");
                String password = URIUtils.getString(queries, PASSWORD_KEY, "");
                return new BasicAuth(username, password, headers);
            case SIG_V4:
                String region = URIUtils.getString(queries, REGION_KEY, "");
                String accessKey = URIUtils.getString(queries, BucketURI.ACCESS_KEY_KEY, "");
                String secretKey = URIUtils.getString(queries, BucketURI.SECRET_KEY_KEY, "");
                return new AwsSigV4Auth(region, accessKey, secretKey, headers);
            case BEARER:
                String token = URIUtils.getString(queries, TOKEN_KEY, "");
                return new BearerTokenAuth(token, headers);
            case AZURE_AD:
                String cloud = URIUtils.getString(queries, CLOUD_KEY, "");
                String clientId = URIUtils.getString(queries, CLIENT_ID_KEY, "");
                String clientSecret = URIUtils.getString(queries, CLIENT_SECRET_KEY, "");
                String tenantId = URIUtils.getString(queries, TENANT_ID_KEY, "");
                return new AzureADAuth(cloud, clientId, clientSecret, tenantId, headers);
            default:
                throw new IllegalArgumentException("Unsupported auth type " + authType);
        }
    }

    private static Map<String, String> getHeaders(Map<String, List<String>> queries) {
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : queries.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(HEADER_PREFIX)) {
                String headerKey = key.substring(HEADER_PREFIX.length());
                String headerValue = entry.getValue().get(0);
                headers.put(headerKey, headerValue);
            }
        }
        return headers;
    }
}
