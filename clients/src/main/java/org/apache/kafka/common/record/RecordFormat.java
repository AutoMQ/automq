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
package org.apache.kafka.common.record;

public enum RecordFormat {
    V0(0), V1(1), V2(2);

    public final byte value;

    RecordFormat(int value) {
        this.value = (byte) value;
    }

    public static RecordFormat lookup(byte version) {
        switch (version) {
            case 0: return V0;
            case 1: return V1;
            case 2: return V2;
            default: throw new IllegalArgumentException("Unknown format version: " + version);
        }
    }

    public static RecordFormat current() {
        return V2;
    }

}
