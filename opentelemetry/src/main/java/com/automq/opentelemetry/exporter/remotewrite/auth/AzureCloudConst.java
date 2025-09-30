package com.automq.opentelemetry.exporter.remotewrite.auth;


import org.apache.commons.lang3.StringUtils;

public class AzureCloudConst {
    public static final String AZURE_CHINA = "azurechina";
    public static final String AZURE_PUBLIC = "azurepublic";
    public static final String AZURE_GOVERNMENT = "azuregovernment";

    public static String azureChinaAudienceManagedIdentity = System.getenv("AZURE_CHINA_AUDIENCE_MANAGED_IDENTITY");
    public static String azurePublicAudienceManagedIdentity = System.getenv("AZURE_PUBLIC_AUDIENCE_MANAGED_IDENTITY");
    public static String azureGovernmentAudienceManagedIdentity = System.getenv("AZURE_GOVERNMENT_AUDIENCE_MANAGED_IDENTITY");

    public static String azureChinaAudienceClientSecret = System.getenv("AZURE_CHINA_AUDIENCE_CLIENT_SECRET");
    public static String azurePublicAudienceClientSecret = System.getenv("AZURE_PUBLIC_AUDIENCE_CLIENT_SECRET");
    public static String azureGovernmentAudienceClientSecret = System.getenv("AZURE_GOVERNMENT_AUDIENCE_CLIENT_SECRET");

    static {
        if (StringUtils.isBlank(azureChinaAudienceManagedIdentity)) {
            azureChinaAudienceManagedIdentity = "https://monitor.azure.cn";
        }
        if (StringUtils.isBlank(azurePublicAudienceManagedIdentity)) {
            azurePublicAudienceManagedIdentity = "https://monitor.azure.com";
        }
        if (StringUtils.isBlank(azureGovernmentAudienceManagedIdentity)) {
            azureGovernmentAudienceManagedIdentity = "https://monitor.azure.us";
        }

        if (StringUtils.isBlank(azureChinaAudienceClientSecret)) {
            azureChinaAudienceClientSecret = "https://monitor.azure.cn//.default";
        }
        if (StringUtils.isBlank(azurePublicAudienceClientSecret)) {
            azurePublicAudienceClientSecret = "https://monitor.azure.com//.default";
        }
        if (StringUtils.isBlank(azureGovernmentAudienceClientSecret)) {
            azureGovernmentAudienceClientSecret = "https://monitor.azure.us//.default";
        }
    }
}
