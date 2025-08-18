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

package com.automq.stream.s3.wal;

import com.automq.stream.utils.IdURI;

public interface WalFactory {

    WriteAheadLog build(IdURI uri, BuildOptions options);


    class BuildOptions {
        private final long nodeEpoch;
        private final OpenMode openMode;

        private BuildOptions(long nodeEpoch, OpenMode openMode) {
            this.nodeEpoch = nodeEpoch;
            this.openMode = openMode;
        }

        public OpenMode openMode() {
            return openMode;
        }

        public long nodeEpoch() {
            return nodeEpoch;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private long nodeEpoch;
            private OpenMode openMode = OpenMode.READ_WRITE;

            public Builder openMode(OpenMode openMode) {
                this.openMode = openMode;
                return this;
            }

            public Builder nodeEpoch(long nodeEpoch) {
                this.nodeEpoch = nodeEpoch;
                return this;
            }

            public BuildOptions build() {
                if (nodeEpoch <= 0) {
                    throw new IllegalArgumentException("The node epoch must be greater than 0");
                }
                return new BuildOptions(nodeEpoch, openMode);
            }
        }
    }
}
