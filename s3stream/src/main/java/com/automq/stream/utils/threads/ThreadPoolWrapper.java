/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.utils.threads;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolWrapper {
    private String name;
    private ThreadPoolExecutor threadPoolExecutor;
    private List<ThreadPoolStatusMonitor> statusPrinters;

    ThreadPoolWrapper(final String name, final ThreadPoolExecutor threadPoolExecutor,
        final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.name = name;
        this.threadPoolExecutor = threadPoolExecutor;
        this.statusPrinters = statusPrinters;
    }

    public static ThreadPoolWrapperBuilder builder() {
        return new ThreadPoolWrapperBuilder();
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    public void setThreadPoolExecutor(final ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public List<ThreadPoolStatusMonitor> getStatusPrinters() {
        return this.statusPrinters;
    }

    public void setStatusPrinters(final List<ThreadPoolStatusMonitor> statusPrinters) {
        this.statusPrinters = statusPrinters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThreadPoolWrapper wrapper = (ThreadPoolWrapper) o;
        return Objects.equal(name, wrapper.name) && Objects.equal(threadPoolExecutor, wrapper.threadPoolExecutor) && Objects.equal(statusPrinters, wrapper.statusPrinters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, threadPoolExecutor, statusPrinters);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("threadPoolExecutor", threadPoolExecutor)
            .add("statusPrinters", statusPrinters)
            .toString();
    }

    public static class ThreadPoolWrapperBuilder {
        private String name;
        private ThreadPoolExecutor threadPoolExecutor;
        private List<ThreadPoolStatusMonitor> statusPrinters;

        ThreadPoolWrapperBuilder() {
        }

        public ThreadPoolWrapperBuilder name(final String name) {
            this.name = name;
            return this;
        }

        public ThreadPoolWrapperBuilder threadPoolExecutor(
            final ThreadPoolExecutor threadPoolExecutor) {
            this.threadPoolExecutor = threadPoolExecutor;
            return this;
        }

        public ThreadPoolWrapperBuilder statusPrinters(
            final List<ThreadPoolStatusMonitor> statusPrinters) {
            this.statusPrinters = statusPrinters;
            return this;
        }

        public ThreadPoolWrapper build() {
            return new ThreadPoolWrapper(this.name, this.threadPoolExecutor, this.statusPrinters);
        }

        @Override
        public String toString() {
            return "ThreadPoolWrapper.ThreadPoolWrapperBuilder(name=" + this.name + ", threadPoolExecutor=" + this.threadPoolExecutor + ", statusPrinters=" + this.statusPrinters + ")";
        }
    }
}
