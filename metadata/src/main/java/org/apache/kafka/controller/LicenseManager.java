package org.apache.kafka.controller;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Date;
import java.util.List;


public interface LicenseManager extends Reconfigurable {

    String describeLicense();

    String exportClusterManifest();

    boolean checkLicense(String license);

    boolean replay(KVRecord record);

    boolean initialized();

    Date getExpireDate();

    void start();

    void shutdown();

    List<ApiMessageAndVersion> getRecordsToAppend(String license);
}
