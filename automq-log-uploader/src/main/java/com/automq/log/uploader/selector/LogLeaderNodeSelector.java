/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.log.uploader.selector;

/**
 * Determines whether the current node should act as the primary S3 log uploader.
 */
public interface LogLeaderNodeSelector {

    /**
     * @return {@code true} if the current node should upload and clean up logs in S3.
     */
    boolean isLeader();

    /**
     * Creates a selector with a static boolean decision.
     *
     * @param primary whether this node should be primary
     * @return selector returning the static decision
     */
    static LogLeaderNodeSelector staticSelector(boolean primary) {
        return () -> primary;
    }
}
