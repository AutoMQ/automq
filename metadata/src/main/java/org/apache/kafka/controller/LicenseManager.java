package org.apache.kafka.controller;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Date;
import java.util.List;
import java.util.Map;


public interface LicenseManager extends Reconfigurable {

    String describeLicense();
    String exportClusterManifest();

    boolean checkLicense(String license);

    boolean replay(KVRecord record);

    boolean replayConfigRecord(ApiMessage record);

    boolean recordExists();

    boolean hasGenesisAnchor();

    boolean legacyUpdateDynamicConfig(Map<ConfigResource, Map<String, String>> newConfigs);

    Date getExpireDate();

    void start();

    void shutdown();

    String installId();

    List<ApiMessageAndVersion> getRecordsToAppend(String license);
}
