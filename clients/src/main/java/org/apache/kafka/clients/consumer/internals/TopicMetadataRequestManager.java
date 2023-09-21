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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * <p>
 * Manages the state of topic metadata requests. This manager returns a
 * {@link NetworkClientDelegate.PollResult} when a request is ready to
 * be sent. Specifically, this manager handles the following user API calls:
 * </p>
 * <ul>
 * <li>listTopics</li>
 * <li>partitionsFor</li>
 * </ul>
 * <p>
 * The manager checks the state of the {@link TopicMetadataRequestState} before sending a new one to
 * prevent sending it without backing off from previous attempts.
 * It also checks the state of inflight requests to avoid overwhelming the broker with duplicate requests.
 * The {@code inflightRequests} are memorized by topic name. If all topics are requested, then we use {@code Optional
 * .empty()} as the key.
 * Once a request is completed successfully, its corresponding entry is removed.
 * </p>
 */

public class TopicMetadataRequestManager implements RequestManager {
    private final boolean allowAutoTopicCreation;
    private final Map<Optional<String>, TopicMetadataRequestState> inflightRequests;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private final Logger log;
    private final LogContext logContext;

    public TopicMetadataRequestManager(final LogContext context, final ConsumerConfig config) {
        logContext = context;
        log = logContext.logger(getClass());
        inflightRequests = new HashMap<>();
        retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        allowAutoTopicCreation = config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG);
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        List<NetworkClientDelegate.UnsentRequest> requests = inflightRequests.values().stream()
            .map(req -> req.send(currentTimeMs))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
        return requests.isEmpty() ?
            new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>()) :
            new NetworkClientDelegate.PollResult(0, Collections.unmodifiableList(requests));
    }

    /**
     * return the future of the metadata request. Return the existing future if a request for the same topic is already
     * inflight.
     *
     * @param topic to be requested. If empty, return the metadata for all topics.
     * @return the future of the metadata request.
     */
    public CompletableFuture<Map<String, List<PartitionInfo>>> requestTopicMetadata(final Optional<String> topic) {
        if (inflightRequests.containsKey(topic)) {
            return inflightRequests.get(topic).future;
        }

        TopicMetadataRequestState newRequest = new TopicMetadataRequestState(
                logContext,
                topic,
                retryBackoffMs,
                retryBackoffMaxMs);
        inflightRequests.put(topic, newRequest);
        return newRequest.future;
    }

    // Visible for testing
    List<TopicMetadataRequestState> inflightRequests() {
        return new ArrayList<>(inflightRequests.values());
    }

    class TopicMetadataRequestState extends RequestState {
        private final Optional<String> topic;
        CompletableFuture<Map<String, List<PartitionInfo>>> future;

        public TopicMetadataRequestState(final LogContext logContext,
                                         final Optional<String> topic,
                                         final long retryBackoffMs,
                                         final long retryBackoffMaxMs) {
            super(logContext, TopicMetadataRequestState.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs);
            future = new CompletableFuture<>();
            this.topic = topic;
        }

        /**
         * prepare the metadata request and return an
         * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest} if needed.
         */
        private Optional<NetworkClientDelegate.UnsentRequest> send(final long currentTimeMs) {
            if (!canSendRequest(currentTimeMs)) {
                return Optional.empty();
            }
            onSendAttempt(currentTimeMs);

            final MetadataRequest.Builder request =
                topic.map(t -> new MetadataRequest.Builder(Collections.singletonList(t), allowAutoTopicCreation))
                    .orElseGet(MetadataRequest.Builder::allTopics);

            return Optional.of(createUnsentRequest(request));
        }

        private NetworkClientDelegate.UnsentRequest createUnsentRequest(
                final MetadataRequest.Builder request) {
            return new NetworkClientDelegate.UnsentRequest(
                    request,
                    Optional.empty(),
                    this::processResponseOrException
            );
        }

        private void processResponseOrException(final ClientResponse response,
                                                final Throwable exception) {
            if (exception == null) {
                handleResponse(response, response.receivedTimeMs());
                return;
            }

            if (exception instanceof RetriableException) {
                // We continue to retry on RetriableException
                // TODO: TimeoutException will continue to retry despite user API timeout.
                onFailedAttempt(response.receivedTimeMs());
            } else {
                completeFutureAndRemoveRequest(new KafkaException(exception));
            }
        }

        private void handleResponse(final ClientResponse response, final long responseTimeMs) {
            try {
                Map<String, List<PartitionInfo>> res = handleTopicMetadataResponse((MetadataResponse) response.responseBody());
                future.complete(res);
                inflightRequests.remove(topic);
            } catch (RetriableException e) {
                onFailedAttempt(responseTimeMs);
            } catch (Exception t) {
                completeFutureAndRemoveRequest(t);
            }
        }

        private void completeFutureAndRemoveRequest(final Throwable throwable) {
            future.completeExceptionally(throwable);
            inflightRequests.remove(topic);
        }

        private Map<String, List<PartitionInfo>> handleTopicMetadataResponse(final MetadataResponse response) {
            Cluster cluster = response.buildCluster();

            final Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
            if (!unauthorizedTopics.isEmpty())
                throw new TopicAuthorizationException(unauthorizedTopics);

            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty()) {
                // if there were errors, we need to check whether they were fatal or whether
                // we should just retry

                log.debug("Topic metadata fetch included errors: {}", errors);

                for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                    String topic = errorEntry.getKey();
                    Errors error = errorEntry.getValue();

                    if (error == Errors.INVALID_TOPIC_EXCEPTION)
                        throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                    else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                        // if a requested topic is unknown, we just continue and let it be absent
                        // in the returned map
                        continue;
                    else if (error.exception() instanceof RetriableException) {
                        throw error.exception();
                    } else
                        throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                            error.exception());
                }
            }

            HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
            for (String topic : cluster.topics())
                topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
            return topicsPartitionInfos;
        }

        public Optional<String> topic() {
            return topic;
        }
    }
}
