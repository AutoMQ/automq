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

package kafka.automq.table.process.convert;

import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.exception.ConverterException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * A decorator for a {@link Converter} that delays its initialization until it is first used.
 *
 * <p>This is useful for deferring expensive initialization logic, such as fetching
 * schema metadata over the network, until a record from the corresponding topic
 * is actually being processed.</p>
 *
 * <p>This class is thread-safe.</p>
 */
public class LazyConverter implements Converter {
    private static final Logger log = LoggerFactory.getLogger(LazyConverter.class);

    private final Supplier<Converter> converterSupplier;
    private volatile Converter delegate;

    /**
     * Creates a new LazyConverter.
     *
     * @param converterSupplier a supplier that provides the actual converter instance when called.
     */
    public LazyConverter(Supplier<Converter> converterSupplier) {
        this.converterSupplier = converterSupplier;
    }

    /**
     * Gets the delegate converter, initializing it if necessary.
     * Uses double-checked locking for thread-safe lazy initialization.
     */
    private Converter getDelegate() {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    Converter localDelegate = converterSupplier.get();
                    if (localDelegate == null) {
                        throw new IllegalStateException("Converter supplier returned null");
                    }
                    log.info("Successfully initialized delegate converter: {}", localDelegate.getClass().getName());
                    delegate = localDelegate;
                }
            }
        }
        return delegate;
    }

    @Override
    public ConversionResult convert(String topic, ByteBuffer buffer) throws ConverterException {
        return getDelegate().convert(topic, buffer);
    }
}
