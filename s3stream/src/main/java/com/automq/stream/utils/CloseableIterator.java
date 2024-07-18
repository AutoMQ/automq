/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package com.automq.stream.utils;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterators that need to be closed in order to release resources should implement this interface.
 * <p>
 * Warning: before implementing this interface, consider if there are better options. The chance of misuse is
 * a bit high since people are used to iterating without closing.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {
    static <R> CloseableIterator<R> wrap(Iterator<R> inner) {
        return new CloseableIterator<R>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public R next() {
                return inner.next();
            }

            @Override
            public void remove() {
                inner.remove();
            }
        };
    }

    void close();
}
