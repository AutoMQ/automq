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

package kafka.log.stream.s3.wal;

import java.util.Objects;

public enum ShutdownType {
    GRACEFULLY(0), // 优雅关闭
    UNGRACEFULLY(1);  // 暴力关闭
    private final Integer code;

    ShutdownType(Integer code) {
        this.code = code;
    }

    public static ShutdownType fromCode(Integer code) {
        for (ShutdownType type : ShutdownType.values()) {
            if (Objects.equals(type.getCode(), code)) {
                return type;
            }
        }
        return null;
    }

    public Integer getCode() {
        return code;
    }
}
