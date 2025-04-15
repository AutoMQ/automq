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

package com.automq.stream.s3.exceptions;

public class ObjectNotExistException extends AutoMQException {

    public ObjectNotExistException() {
    }

    public ObjectNotExistException(long objectId) {
        super("Object not exist: " + objectId);
    }

    public ObjectNotExistException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

}
