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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.S3Error;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class S3DeleteResponseHandler implements DeleteResponseHandler<DeleteObjectsResponse> {
    private static final String S3_API_NO_SUCH_KEY = "NoSuchKey";

    public void setDeleteObjectsReturnSuccessKeys(boolean deleteObjectsReturnSuccessKeys) {
        this.deleteObjectsReturnSuccessKeys = deleteObjectsReturnSuccessKeys;
    }

    private  boolean deleteObjectsReturnSuccessKeys = false;



    @Override
    public List<String> handleDeleteResponse(List<String> objectKeys, TimerUtil timerUtil, DeleteObjectsResponse response) {
        Set<String> successDeleteKeys = new HashSet<>();
        int errDeleteCount = 0;
        boolean hasUnExpectedResponse = false;
        if (deleteObjectsReturnSuccessKeys) {
            response.deleted().stream().map(DeletedObject::key).forEach(successDeleteKeys::add);

            // expect NoSuchKey is not response because s3 api won't return this in errors.
            for (S3Error error : response.errors()) {
                LOGGER.error("[ControllerS3Operator]: Delete objects for key [{}] error code [{}] message [{}]",
                    error.key(), error.code(), error.message());
                errDeleteCount++;
            }

        } else {
            // deleteObjects not return successKeys think as all success.
            successDeleteKeys.addAll(objectKeys);

            for (S3Error error : response.errors()) {
                if (S3_API_NO_SUCH_KEY.equals(error.code())) {
                    // ignore for delete objects.
                    continue;
                }

                if (errDeleteCount < 30) {
                    LOGGER.error("[ControllerS3Operator]: Delete objects for key [{}] error code [{}] message [{}]",
                        error.key(), error.code(), error.message());
                }

                if (!StringUtils.isEmpty(error.key())) {
                    successDeleteKeys.remove(error.key());
                } else {
                    hasUnExpectedResponse = true;
                }

                errDeleteCount++;
            }

            if (hasUnExpectedResponse) {
                successDeleteKeys = Collections.emptySet();
            }
        }

        LOGGER.info("[ControllerS3Operator]: Delete objects finished, count: {}, errCount: {}, cost: {}",
            successDeleteKeys.size(), errDeleteCount, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));

        if (!hasUnExpectedResponse) {
            S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        } else {
            S3OperationStats.getInstance().deleteObjectsStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        }

        return new ArrayList<>(successDeleteKeys);
    }
}
