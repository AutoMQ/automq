/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller.stream;

/**
 * The S3ObjectKeyGeneratorManager manages all S3Object
 */
public final class S3ObjectKeyGeneratorManager {

    public static class GenerateContext {
        protected Long objectId;
        protected GenerateContext(Long objectId) {
            this.objectId = objectId;
        }
    }
    interface S3ObjectKeyGenerator {
        String generate(GenerateContext context);
    }
    public static S3ObjectKeyGenerator getByVersion(int version) {
        switch (version) {
            case 0: return generatorV0;
            default: throw new IllegalArgumentException("Unsupported version " + version);
        }
    }

    public static class GenerateContextV0 extends GenerateContext {
        private String clusterName;

        GenerateContextV0(String clusterName, Long objectId) {
            super(objectId);
            this.clusterName = clusterName;
        }
    }

    static S3ObjectKeyGenerator generatorV0 = (GenerateContext ctx) -> {
        if (!(ctx instanceof GenerateContextV0)) {
            throw new IllegalArgumentException("Unsupported context " + ctx.getClass().getName());
        }
        GenerateContextV0 ctx0 = (GenerateContextV0) ctx;
        return String.format("%s/%s/%d", ctx0.objectId.hashCode(), ctx0.clusterName, ctx0.objectId);
    };
}
