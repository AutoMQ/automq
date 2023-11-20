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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.FailoverContextRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class FailoverContextImage {
    public static final FailoverContextImage EMPTY = new FailoverContextImage(Collections.emptyMap());

    private final Map<Integer, FailoverContextRecord> contexts;

    public FailoverContextImage(final Map<Integer, FailoverContextRecord> contexts) {
        this.contexts = contexts;
    }

    public Map<Integer, FailoverContextRecord> contexts() {
        return contexts;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        contexts.values().forEach(r -> writer.write(0, r));
    }

    public boolean isEmpty() {
        return contexts.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailoverContextImage that = (FailoverContextImage) o;
        return Objects.equals(contexts, that.contexts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contexts);
    }
}
