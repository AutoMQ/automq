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

package kafka.log.es;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class FutureUtil {
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        cf.completeExceptionally(ex);
        return cf;
    }

    public static void suppress(Runnable run, Logger logger) {
        try {
            run.run();
        } catch (Throwable t) {
            logger.error("Suppress error", t);
        }
    }

    /**
     * Propagate CompleteFuture result / error from source to dest.
     */
    public static <T> void propagate(CompletableFuture<T> source, CompletableFuture<T> dest) {
        source.whenComplete((rst, ex) -> {
            if (ex != null) {
                dest.completeExceptionally(ex);
            } else {
                dest.complete(rst);
            }
        });
    }

    /**
     * Catch exceptions as a last resort to avoid unresponsiveness.
     */
    public static <T> CompletableFuture<T> exec(Supplier<CompletableFuture<T>> run, Logger logger, String name) {
        try {
            return run.get();
        } catch (Throwable ex) {
            logger.error("{} run with unexpected exception", name, ex);
            return failedFuture(ex);
        }
    }

    /**
     * Catch exceptions as a last resort to avoid unresponsiveness.
     */
    public static <T> void exec(Runnable run, CompletableFuture<T> cf, Logger logger, String name) {
        try {
            run.run();
        } catch (Throwable ex) {
            logger.error("{} run with unexpected exception", name, ex);
            cf.completeExceptionally(ex);
        }
    }

    public static Throwable cause(Throwable ex) {
        if (ex instanceof ExecutionException) {
            if (ex.getCause() != null) {
                return cause(ex.getCause());
            } else {
                return ex;
            }
        } else if (ex instanceof CompletionException) {
            if (ex.getCause() != null) {
                return cause(ex.getCause());
            } else {
                return ex;
            }
        }
        return ex;
    }
}
