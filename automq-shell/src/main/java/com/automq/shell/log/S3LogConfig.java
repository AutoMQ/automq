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

package com.automq.shell.log;

public interface S3LogConfig {

    boolean isEnabled();

    boolean isActiveController();

    String clusterId();

    int nodeId();

    String s3Endpoint();

    String s3Region();

    String s3OpsBucket();

    boolean s3PathStyle();
}
