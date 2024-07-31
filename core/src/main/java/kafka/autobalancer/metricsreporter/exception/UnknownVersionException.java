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

package kafka.autobalancer.metricsreporter.exception;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException.
 */
/*
 Unknown version during Serialization/Deserialization.
 */
public class UnknownVersionException extends Exception {
    public UnknownVersionException(String msg) {
        super(msg);
    }
}
