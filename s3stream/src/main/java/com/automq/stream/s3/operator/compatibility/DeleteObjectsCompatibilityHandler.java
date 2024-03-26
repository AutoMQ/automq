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

package com.automq.stream.s3.operator.compatibility;

import com.automq.stream.s3.metrics.TimerUtil;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;

import java.util.List;

public interface DeleteObjectsCompatibilityHandler {
    List<String> handleDeleteObjectsResponse(List<String> objectKeys, TimerUtil timerUtil, DeleteObjectsResponse response);
}