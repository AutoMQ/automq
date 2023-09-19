/*
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

package kafka.log.stream.api;

import kafka.log.stream.utils.Arguments;

public class OpenStreamOptions {
    private WriteMode writeMode = WriteMode.SINGLE;
    private ReadMode readMode = ReadMode.MULTIPLE;
    private long epoch;

    public static Builder newBuilder() {
        return new Builder();
    }

    public WriteMode writeMode() {
        return writeMode;
    }

    public ReadMode readMode() {
        return readMode;
    }

    public long epoch() {
        return epoch;
    }

    public enum WriteMode {
        SINGLE(0), MULTIPLE(1);

        final int code;

        WriteMode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public enum ReadMode {
        SINGLE(0), MULTIPLE(1);

        final int code;

        ReadMode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public static class Builder {
        private final OpenStreamOptions options = new OpenStreamOptions();

        public Builder writeMode(WriteMode writeMode) {
            Arguments.isNotNull(writeMode, "WriteMode should be set with SINGLE or MULTIPLE");
            options.writeMode = writeMode;
            return this;
        }

        public Builder readMode(ReadMode readMode) {
            Arguments.isNotNull(readMode, "ReadMode should be set with SINGLE or MULTIPLE");
            options.readMode = readMode;
            return this;
        }

        public Builder epoch(long epoch) {
            options.epoch = epoch;
            return this;
        }

        public OpenStreamOptions build() {
            return options;
        }
    }
}
