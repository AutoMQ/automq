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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.utils.Timer;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This class interfaces with the KafkaConsumer and the background thread. It allows the caller to enqueue events via
 * the {@code add()} method and to retrieve events via the {@code poll()} method.
 */
public interface EventHandler extends Closeable {
    /**
     * Retrieves and removes a {@link BackgroundEvent}. Returns an empty Optional instance if there is nothing.
     * @return an Optional of {@link BackgroundEvent} if the value is present. Otherwise, an empty Optional.
     */
    Optional<BackgroundEvent> poll();

    /**
     * Check whether there are pending {@code BackgroundEvent} await to be consumed.
     * @return true if there are no pending event
     */
    boolean isEmpty();

    /**
     * Add an {@link ApplicationEvent} to the handler. The method returns true upon successful add; otherwise returns
     * false.
     * @param event     An {@link ApplicationEvent} created by the polling thread.
     * @return          true upon successful add.
     */
    boolean add(ApplicationEvent event);

    /**
     * Add a {@link CompletableApplicationEvent} to the handler. The method blocks waiting for the result, and will
     * return the result value upon successful completion; otherwise throws an error.
     *
     * <p/>
     *
     * See {@link CompletableApplicationEvent#get(Timer)} and {@link Future#get(long, TimeUnit)} for more details.
     *
     * @param event A {@link CompletableApplicationEvent} created by the polling thread.
     * @param timer Timer for which to wait for the event to complete
     * @return      Value that is the result of the event
     * @param <T>   Type of return value of the event
     */
    <T> T addAndGet(final CompletableApplicationEvent<T> event, final Timer timer);
}
