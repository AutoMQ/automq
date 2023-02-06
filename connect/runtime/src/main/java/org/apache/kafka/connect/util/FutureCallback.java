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
package org.apache.kafka.connect.util;

/**
 * An implementation of {@link ConvertingFutureCallback} that doesn't do any conversion - i.e. the callback result is
 * returned transparently via {@link #get}.
 */
public class FutureCallback<T> extends ConvertingFutureCallback<T, T> {

    public FutureCallback(Callback<T> underlying) {
        super(underlying);
    }

    public FutureCallback() {
        super(null);
    }

    @Override
    public T convert(T result) {
        return result;
    }
}
