/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.kshell.log.uploader;

/**
 * @author ipsum-0320
 */
public interface Uploader {
    String PREFIX = "logs/";
    String POSTFIX = ".log";
    String SHUTDOWN_SIGNAL = "shutdown-signal";
    int MAX_RETRY = 10;
    String BUFFER_FILE_NAME = "buffer-file.log";
    int BUFFER_SIZE = 16 * 1024 * 1024;

    void toBuffer(byte[] value);

    void upload(String key);

    void close();

    String wrapKey(String key);

}
