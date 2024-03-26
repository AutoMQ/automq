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

package com.automq.stream.s3.operator.compatibility.bos;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.s3.operator.compatibility.DeleteObjectsCompatibilityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Error;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
/**
 * OSSProvider: BOS
 * DOC: https://cloud.baidu.com/doc/BOS/s/tkc5twspg
 * API: DeleteMultipleObjects
 * Description: BOS default deleteObjects works in quiet mode which won't return success keys.
 * and if object not exist errors will occur in response.
 */
public class BOSDeleteObjectsCompatibilityHandler implements DeleteObjectsCompatibilityHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BOSDeleteObjectsCompatibilityHandler.class);

    /**
     * handleDeleteObjectsResponse which will handle deleteObjects response from oss provider and return success delete object keys.
     * @param objectKeys need to delete object keys in request.
     * @param timerUtil use for record metrics.
     * @param response origin response return from aws s3 sdk
     * @return success delete object keys.
     */
    @Override
    public List<String> handleDeleteObjectsResponse(List<String> objectKeys, TimerUtil timerUtil, DeleteObjectsResponse response) {
        List<String> ans = null;
        int errDeleteCount = 0;

        if (!response.hasErrors()) {
            ans = objectKeys;
        } else {
            Set<String> successDeleteKeys = new HashSet<>(objectKeys);
            Set<String> errorDeleteKeys = null;

            for (S3Error error : response.errors()) {
                if (!error.code().equals("NoSuchKey")) {
                    LOGGER.error("[ControllerS3Operator]: Delete objects for key {} error code {} message {}", error.key(), error.code(), error.message());
                    if (errorDeleteKeys == null) {
                        errorDeleteKeys = new HashSet<>();
                    }
                    errorDeleteKeys.add(error.key());
                    successDeleteKeys.remove(error.key());
                }
            }

            ans = new ArrayList<>(successDeleteKeys);
            errDeleteCount = errorDeleteKeys == null ? 0 : errorDeleteKeys.size();
        }

        LOGGER.info("[ControllerS3Operator]: Delete objects finished, count: {}, errCount: {}, cost: {}", ans.size(), errDeleteCount, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));

        S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));

        return ans;
    }
}
