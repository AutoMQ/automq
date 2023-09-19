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
package kafka.log.streamaspect;

import io.netty.util.concurrent.FastThreadLocal;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReadManualReleaseHint {

    public static final FastThreadLocal<AtomicBoolean> MANUAL_RELEASE = new FastThreadLocal<AtomicBoolean>() {
        @Override
        protected AtomicBoolean initialValue() {
            return new AtomicBoolean(false);
        }
    };

    public static boolean isMarked() {
        return MANUAL_RELEASE.get().get();
    }

    public static void mark() {
        MANUAL_RELEASE.get().set(true);
    }

    public static void reset() {
        MANUAL_RELEASE.get().set(false);
    }

}
