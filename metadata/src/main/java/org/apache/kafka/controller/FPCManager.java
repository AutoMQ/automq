package org.apache.kafka.controller;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.protocol.ApiMessage;

import java.util.Date;
import java.util.Map;


public interface FPCManager extends Reconfigurable {

    boolean checkLicense();

    boolean replayKVRecord(KVRecord record);

    boolean replayConfigRecord(ApiMessage record);

    boolean recordExists();

    boolean hasGenesisAnchor();

    boolean legacyUpdateDynamicConfig(Map<ConfigResource, Map<String, String>> newConfigs);

    Date getExpireDate();

    void start();

    void close();

    String installId();
}
