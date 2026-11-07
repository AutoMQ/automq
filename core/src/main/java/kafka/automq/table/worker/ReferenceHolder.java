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

package kafka.automq.table.worker;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class ReferenceHolder<T> extends AbstractReferenceCounted {
    private final T value;
    private final Runnable deallocate;

    public ReferenceHolder(T value, Runnable deallocate) {
        this.value = value;
        this.deallocate = deallocate;
    }

    public T value() {
        return value;
    }

    @Override
    protected void deallocate() {
        deallocate.run();
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }
}
