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

package kafka.automq.backpressure;

/**
 * The Regulator class is responsible for controlling and limiting the rate of external requests.
 * It provides methods to increase, decrease, and minimize the flow of incoming requests.
 */
public interface Regulator {

    /**
     * Increase the rate of incoming requests.
     * If the rate is already at the maximum, this method does nothing.
     */
    void increase();

    /**
     * Decrease the rate of incoming requests.
     * If the rate is already at the minimum, this method does nothing.
     */
    void decrease();
}
