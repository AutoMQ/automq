/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils.threads;

import java.util.concurrent.ThreadPoolExecutor;

public interface ThreadPoolStatusMonitor {

    String describe();

    double value(ThreadPoolExecutor executor);

    boolean needPrintJstack(ThreadPoolExecutor executor, double value);
}
