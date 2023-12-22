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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;
import static org.apache.kafka.common.protocol.Errors.COORDINATOR_LOAD_IN_PROGRESS;

public class CommitRequestManager implements RequestManager, MemberStateListener {

    private final SubscriptionState subscriptions;
    private final LogContext logContext;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final long retryBackoffMs;
    private final String groupId;
    private final Optional<String> groupInstanceId;
    private final long retryBackoffMaxMs;
    // For testing only
    private final OptionalDouble jitter;
    private final boolean throwOnFetchStableOffsetUnsupported;
    final PendingRequests pendingRequests;
    private boolean closing = false;

    /**
     *  Latest member ID and epoch received via the {@link #onMemberEpochUpdated(Optional, Optional)},
     *  to be included in the OffsetFetch and OffsetCommit requests if present. This will have
     *  the latest values received from the broker, or empty of the member is not part of the
     *  group anymore.
     */
    private final MemberInfo memberInfo;

    public CommitRequestManager(
            final Time time,
            final LogContext logContext,
            final SubscriptionState subscriptions,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final String groupId,
            final Optional<String> groupInstanceId) {
        this(time, logContext, subscriptions, config, coordinatorRequestManager, groupId,
                groupInstanceId, config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG), OptionalDouble.empty());
    }

    // Visible for testing
    CommitRequestManager(
        final Time time,
        final LogContext logContext,
        final SubscriptionState subscriptions,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final String groupId,
        final Optional<String> groupInstanceId,
        final long retryBackoffMs,
        final long retryBackoffMaxMs,
        final OptionalDouble jitter) {
        Objects.requireNonNull(coordinatorRequestManager, "Coordinator is needed upon committing offsets");
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
        this.pendingRequests = new PendingRequests();
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            final long autoCommitInterval =
                Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval));
        } else {
            this.autoCommitState = Optional.empty();
        }
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.subscriptions = subscriptions;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.jitter = jitter;
        this.throwOnFetchStableOffsetUnsupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        this.memberInfo = new MemberInfo();
    }

    /**
     * Poll for the {@link OffsetFetchRequest} and {@link OffsetCommitRequest} request if there's any. The function will
     * also try to autocommit the offsets, if feature is enabled.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // poll only when the coordinator node is known.
        if (!coordinatorRequestManager.coordinator().isPresent())
            return EMPTY;

        if (closing) {
            return drainPendingOffsetCommitRequests();
        }

        maybeAutoCommitAllConsumedAsync();
        if (!pendingRequests.hasUnsentRequests())
            return EMPTY;

        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drain(currentTimeMs);
        // min of the remainingBackoffMs of all the request that are still backing off
        final long timeUntilNextPoll = Math.min(
            findMinTime(unsentOffsetCommitRequests(), currentTimeMs),
            findMinTime(unsentOffsetFetchRequests(), currentTimeMs));
        return new NetworkClientDelegate.PollResult(timeUntilNextPoll, requests);
    }

    @Override
    public void signalClose() {
        closing = true;
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        return autoCommitState.map(ac -> ac.remainingMs(currentTimeMs)).orElse(Long.MAX_VALUE);
    }

    private static long findMinTime(final Collection<? extends RequestState> requests, final long currentTimeMs) {
        return requests.stream()
            .mapToLong(request -> request.remainingBackoffMs(currentTimeMs))
            .min()
            .orElse(Long.MAX_VALUE);
    }

    /**
     * Generate a request to commit offsets if auto-commit is enabled. The request will be
     * returned to be sent out on the next call to {@link #poll(long)}. This will only generate a
     * request if there is no other commit request already in-flight, and if the commit interval
     * has elapsed.
     *
     * @param offsets           Offsets to commit
     * @param expirationTimeMs  Time until which the request will continue to be retried if it
     *                          fails with a retriable error. If not present, the request will be
     *                          sent but not retried.
     * @param checkInterval     True if the auto-commit interval expiration should be checked for
     *                          sending a request. If true, the request will be sent only if the
     *                          auto-commit interval has expired. Pass false to
     *                          send the auto-commit request regardless of the interval (ex.
     *                          auto-commit before rebalance).
     * @param retryOnStaleEpoch True if the request should be retried in case it fails with
     *                          {@link Errors#STALE_MEMBER_EPOCH}.
     * @return Future that will complete when a response is received for the request, or a
     * completed future if no request is generated.
     */
    private CompletableFuture<Void> maybeAutoCommit(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                    final Optional<Long> expirationTimeMs,
                                                    boolean checkInterval,
                                                    boolean retryOnStaleEpoch) {
        if (!autoCommitEnabled()) {
            log.debug("Skipping auto-commit because auto-commit config is not enabled.");
            return CompletableFuture.completedFuture(null);
        }

        AutoCommitState autocommit = autoCommitState.get();
        if (checkInterval && !autocommit.shouldAutoCommit()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> result = addOffsetCommitRequest(offsets, expirationTimeMs, retryOnStaleEpoch)
            .whenComplete(autoCommitCallback(offsets));
        autocommit.resetTimer();
        autocommit.setInflightCommitStatus(true);
        return result;
    }

    /**
     * If auto-commit is enabled, this will generate a commit offsets request for all assigned
     * partitions and their current positions. Note on auto-commit timers: this will reset the
     * auto-commit timer to the interval before issuing the async commit, and when the async commit
     * completes, it will reset the auto-commit timer with the exponential backoff if the request
     * failed with a retriable error.
     *
     * @return Future that will complete when a response is received for the request, or a
     * completed future if no request is generated.
     */
    public CompletableFuture<Void> maybeAutoCommitAllConsumedAsync() {
        if (!autoCommitEnabled()) {
            // Early return to ensure that no action/logging is performed.
            return CompletableFuture.completedFuture(null);
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = subscriptions.allConsumed();
        CompletableFuture<Void> result = maybeAutoCommit(offsets, Optional.empty(), true, true);
        result.whenComplete((__, error) -> {
            if (error != null) {
                if (error instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error.", offsets, error);
                    resetAutoCommitTimer(retryBackoffMs);
                } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, error.getMessage());
                }
            } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });

        return result;
    }

    /**
     * Commit consumed offsets if auto-commit is enabled. Retry while the timer is not expired,
     * until the request succeeds or fails with a fatal error.
     */
    public CompletableFuture<Void> maybeAutoCommitAllConsumedNow(
        final Optional<Long> expirationTimeMs,
        final boolean retryOnStaleEpoch) {
        return maybeAutoCommit(subscriptions.allConsumed(), expirationTimeMs, false, retryOnStaleEpoch);
    }

    private BiConsumer<? super Void, ? super Throwable> autoCommitCallback(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        return (response, throwable) -> {
            autoCommitState.ifPresent(autoCommitState -> autoCommitState.setInflightCommitStatus(false));
            if (throwable == null) {
                log.debug("Completed asynchronous auto-commit of offsets {}", allConsumedOffsets);
            } else if (throwable instanceof RetriableCommitFailedException) {
                log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}",
                        allConsumedOffsets, throwable.getMessage());
            } else {
                log.warn("Asynchronous auto-commit of offsets {} failed", allConsumedOffsets, throwable);
            }
        };
    }

    /**
     * Handles {@link org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent}. It creates an
     * {@link OffsetCommitRequestState} and enqueue it to send later.
     */
    public CompletableFuture<Void> addOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                          final Optional<Long> expirationTimeMs,
                                                          final boolean retryOnStaleEpoch) {
        if (offsets.isEmpty()) {
            log.debug("Skipping commit of empty offsets");
            return CompletableFuture.completedFuture(null);
        }
        return pendingRequests.addOffsetCommitRequest(offsets, expirationTimeMs, retryOnStaleEpoch).future;
    }

    /**
     * Handles {@link org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsApplicationEvent}. It creates an
     * {@link OffsetFetchRequestState} and enqueue it to send later.
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(
        final Set<TopicPartition> partitions,
        final long expirationTimeMs) {
        return pendingRequests.addOffsetFetchRequest(partitions, expirationTimeMs);
    }

    public void updateAutoCommitTimer(final long currentTimeMs) {
        this.autoCommitState.ifPresent(t -> t.updateTimer(currentTimeMs));
    }

    // Visible for testing
    Queue<OffsetCommitRequestState> unsentOffsetCommitRequests() {
        return pendingRequests.unsentOffsetCommits;
    }

    private List<OffsetFetchRequestState> unsentOffsetFetchRequests() {
        return pendingRequests.unsentOffsetFetches;
    }

    private void handleCoordinatorDisconnect(Throwable exception, long currentTimeMs) {
        if (exception instanceof DisconnectException) {
            coordinatorRequestManager.markCoordinatorUnknown(exception.getMessage(), currentTimeMs);
        }
    }

    /**
     * Update latest member ID and epoch used by the member.
     *
     * @param memberEpoch New member epoch received. To be included in the new request.
     * @param memberId Current member ID. To be included in the new request.
     */
    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpoch, Optional<String> memberId) {
        memberInfo.memberId = memberId;
        memberInfo.memberEpoch = memberEpoch;
    }

    /**
     * @return True if auto-commit is enabled as defined in the config {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
     */
    public boolean autoCommitEnabled() {
        return autoCommitState.isPresent();
    }

    /**
     * Reset the auto-commit timer to the auto-commit interval, so that the next auto-commit is
     * sent out on the interval starting from now. If auto-commit is not enabled this will
     * perform no action.
     */
    public void resetAutoCommitTimer() {
        autoCommitState.ifPresent(AutoCommitState::resetTimer);
    }

    /**
     * Reset the auto-commit timer to the provided time (backoff), so that the next auto-commit is
     * sent out then. If auto-commit is not enabled this will perform no action.
     */
    public void resetAutoCommitTimer(long retryBackoffMs) {
        autoCommitState.ifPresent(s -> s.resetTimer(retryBackoffMs));
    }

    /**
     * Drains the inflight offsetCommits during shutdown because we want to make sure all pending commits are sent
     * before closing.
     */
    public NetworkClientDelegate.PollResult drainPendingOffsetCommitRequests() {
        if (pendingRequests.unsentOffsetCommits.isEmpty())
            return EMPTY;
        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drainPendingCommits();
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, requests);
    }

    private class OffsetCommitRequestState extends RetriableRequestState {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final String groupId;
        private final Optional<String> groupInstanceId;

        private final CompletableFuture<Void> future;

        /**
         * Time until which the request should be retried if it fails with retriable
         * errors. If not present, the request is triggered without waiting for a response or
         * retrying.
         */
        private final Optional<Long> expirationTimeMs;

        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final Optional<Long> expirationTimeMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final MemberInfo memberInfo,
                                 final boolean retryOnStaleEpoch) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, memberInfo, retryOnStaleEpoch);
            this.offsets = offsets;
            this.groupId = groupId;
            this.groupInstanceId = groupInstanceId;
            this.future = new CompletableFuture<>();
            this.expirationTimeMs = expirationTimeMs;
        }

        // Visible for testing
        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final Optional<Long> expirationTimeMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final double jitter,
                                 final MemberInfo memberInfo,
                                 final boolean retryOnStaleEpoch) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2,
                retryBackoffMaxMs, jitter, memberInfo, retryOnStaleEpoch);
            this.offsets = offsets;
            this.groupId = groupId;
            this.groupInstanceId = groupInstanceId;
            this.future = new CompletableFuture<>();
            this.expirationTimeMs = expirationTimeMs;
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = entry.getValue();

                OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                            .setName(topicPartition.topic())
                    );

                topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
                );
                requestTopicDataMap.put(topicPartition.topic(), topic);
            }

            OffsetCommitRequestData data = new OffsetCommitRequestData()
                    .setGroupId(this.groupId)
                    .setGroupInstanceId(groupInstanceId.orElse(null))
                    .setTopics(new ArrayList<>(requestTopicDataMap.values()));
            if (memberInfo.memberId.isPresent()) {
                data = data.setMemberId(memberInfo.memberId.get());
            }
            if (memberInfo.memberEpoch.isPresent()) {
                data = data.setGenerationIdOrMemberEpoch(memberInfo.memberEpoch.get());
            }

            OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(data);

            NetworkClientDelegate.UnsentRequest resp = new NetworkClientDelegate.UnsentRequest(
                builder,
                coordinatorRequestManager.coordinator());
            resp.whenComplete(
                (response, throwable) -> {
                    try {
                        if (throwable == null) {
                            log.debug("OffsetCommit response received for offsets {} ", offsets);
                            onResponse(response);
                        } else {
                            log.debug("OffsetCommit completed with error for offsets {}", offsets, throwable);
                            long currentTimeMs = resp.handler().completionTimeMs();
                            handleCoordinatorDisconnect(throwable, currentTimeMs);
                            if (throwable instanceof RetriableException) {
                                maybeRetry(currentTimeMs, throwable);
                            } else {
                                future.completeExceptionally(throwable);
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Unexpected error when completing offset commit: {}", this, t);
                        future.completeExceptionally(t);
                    }
                });
            return resp;
        }

        public void onResponse(final ClientResponse response) {
            long currentTimeMs = response.receivedTimeMs();
            OffsetCommitResponse commitResponse = (OffsetCommitResponse) response.responseBody();
            Set<String> unauthorizedTopics = new HashSet<>();
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = offsets.get(tp);
                    long offset = offsetAndMetadata.offset();
                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        log.debug("OffsetCommit completed successfully for offset {} partition {}", offset, tp);
                        continue;
                    }

                    if (error == Errors.COORDINATOR_NOT_AVAILABLE ||
                        error == Errors.NOT_COORDINATOR ||
                        error == Errors.REQUEST_TIMED_OUT) {
                        coordinatorRequestManager.markCoordinatorUnknown(error.message(), currentTimeMs);
                        maybeRetry(currentTimeMs, error.exception());
                        return;
                    } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS ||
                        error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        // just retry
                        maybeRetry(currentTimeMs, error.exception());
                        return;
                    } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                        log.error("OffsetCommit failed with {} on partition {} for offset {}",
                            error, tp, offset);
                        future.completeExceptionally(new CommitFailedException("OffsetCommit " +
                            "failed with unknown member ID. " + error.message()));
                        return;
                    } else if (error == Errors.STALE_MEMBER_EPOCH) {
                        if (maybeRetryWithNewMemberEpoch(currentTimeMs, error)) {
                            log.debug("OffsetCommit failed with {} and will be retried with the " +
                                "latest member ID and epoch.", error);
                            return;
                        }
                        future.completeExceptionally(commitExceptionForStaleMemberEpoch());
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        // Collect all unauthorized topics before failing
                        unauthorizedTopics.add(tp.topic());
                    } else {
                        log.error("OffsetCommit failed on partition {} for offset {}: {}",
                            tp, offset, error.message());
                        if (error.exception() instanceof RetriableException) {
                            maybeRetry(currentTimeMs, error.exception());
                            return;
                        }
                        future.completeExceptionally(error.exception());
                        return;
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("OffsetCommit failed due to not authorized to commit to topics {}", unauthorizedTopics);
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }

        /**
         * Enqueue the request to be retried with exponential backoff, if the request allows
         * retries and the timer has not expired. Complete the request future exceptionally if
         * the request won't be retried.
         */
        @Override
        void maybeRetry(long currentTimeMs, Throwable throwable) {
            if (!allowsRetries()) {
                // Fail requests that do not allow retries (async requests), making sure to
                // propagate a RetriableCommitException if the failure is retriable.
                future.completeExceptionally(commitExceptionForRetriableError(throwable));
                return;
            }
            if (isExpired(currentTimeMs)) {
                // Fail requests that allowed retries (sync requests), but expired.
                future.completeExceptionally(throwable);
                return;
            }

            // Enqueue request to be retried with backoff. Note that this maintains the same
            // timer of the initial request, so all the retries are time-bounded.
            onFailedAttempt(currentTimeMs);
            pendingRequests.addOffsetCommitRequest(this);
        }

        private boolean isExpired(final long currentTimeMs) {
            return expirationTimeMs.isPresent() && expirationTimeMs.get() <= currentTimeMs;
        }

        /**
         * @return True if the requests allows to be retried (sync requests that provide an
         * expiration time to bound the retries). False if the request does not allow to be
         * retried on RetriableErrors (async requests that does not provide an expiration time
         * for retries)
         */
        private boolean allowsRetries() {
            return expirationTimeMs.isPresent();
        }

        /**
         * Complete the request future with a TimeoutException if the request expired. No action
         * taken if the request is still active.
         *
         * @return True if the request expired.
         */
        private boolean maybeExpire(final long currentTimeMs) {
            if (isExpired(currentTimeMs)) {
                future.completeExceptionally(new TimeoutException("OffsetCommit could not complete " +
                    "before timeout expired."));
                return true;
            }
            return false;
        }

        /**
         * @return A RetriableCommitFailedException for async commit requests if the original
         * Exception was a RetriableException. Return the original one in any other case.
         */
        private Throwable commitExceptionForRetriableError(Throwable throwable) {
            if (!allowsRetries() && throwable instanceof RetriableException)
                return new RetriableCommitFailedException(throwable);
            return throwable;
        }

        private Throwable commitExceptionForStaleMemberEpoch() {
            if (retryOnStaleEpoch)
                return new RetriableCommitFailedException(Errors.STALE_MEMBER_EPOCH.exception());
            return new CommitFailedException("OffsetCommit failed with stale member epoch." + Errors.STALE_MEMBER_EPOCH.message());
        }
    }

    /**
     * Represents a request that can be retried or aborted, based on member ID and epoch
     * information.
     */
    abstract static class RetriableRequestState extends RequestState {

        /**
         * Member info (ID and epoch) to be included in the request if present.
         */
        final MemberInfo memberInfo;

        /**
         * True if the request should be retried if it fails with {@link Errors#STALE_MEMBER_EPOCH}.
         */
        boolean retryOnStaleEpoch;

        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs,
                              long retryBackoffMaxMs, MemberInfo memberInfo, boolean retryOnStaleEpoch) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs);
            this.memberInfo = memberInfo;
            this.retryOnStaleEpoch = retryOnStaleEpoch;
        }

        // Visible for testing
        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs, int retryBackoffExpBase,
                              long retryBackoffMaxMs, double jitter, MemberInfo memberInfo,
                              boolean retryOnStaleEpoch) {
            super(logContext, owner, retryBackoffMs, retryBackoffExpBase, retryBackoffMaxMs, jitter);
            this.memberInfo = memberInfo;
            this.retryOnStaleEpoch = retryOnStaleEpoch;
        }

        /**
         * Retry with backoff if the request failed with {@link Errors#STALE_MEMBER_EPOCH} and
         * the member has valid epoch.
         *
         * @return True if the request has been enqueued to be retried with the latest member ID
         * and epoch.
         */
        boolean maybeRetryWithNewMemberEpoch(long currentTimeMs, Errors responseError) {
            if (retryOnStaleEpoch && memberInfo.memberEpoch.isPresent()) {
                // Request failed with invalid epoch, but the member has a valid one, so
                // retry the request with the latest ID/epoch.
                maybeRetry(currentTimeMs, responseError.exception());
                return true;
            }
            return false;
        }

        abstract void maybeRetry(long currentTimeMs, Throwable throwable);
    }

    class OffsetFetchRequestState extends RetriableRequestState {

        /**
         * Partitions to get committed offsets for.
         */
        public final Set<TopicPartition> requestedPartitions;

        private final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        /**
         * Time until which the request should be retried if it fails with retriable errors.
         */
        private final long expirationTimeMs;

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final long expirationTimeMs,
                                       final MemberInfo memberInfo) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, memberInfo, true);
            this.requestedPartitions = partitions;
            this.future = new CompletableFuture<>();
            this.expirationTimeMs = expirationTimeMs;
        }

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final long expirationTimeMs,
                                       final double jitter,
                                       final MemberInfo memberInfo) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2,
                retryBackoffMaxMs, jitter, memberInfo, true);
            this.requestedPartitions = partitions;
            this.future = new CompletableFuture<>();
            this.expirationTimeMs = expirationTimeMs;
        }

        public boolean sameRequest(final OffsetFetchRequestState request) {
            return requestedPartitions.equals(request.requestedPartitions);
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {

            OffsetFetchRequest.Builder builder;
            if (memberInfo.memberId.isPresent() && memberInfo.memberEpoch.isPresent()) {
                builder = new OffsetFetchRequest.Builder(
                        groupId,
                        memberInfo.memberId.get(),
                        memberInfo.memberEpoch.get(),
                        true,
                        new ArrayList<>(this.requestedPartitions),
                        throwOnFetchStableOffsetUnsupported);
            } else {
                // Building request without passing member ID/epoch to leave the logic to choose
                // default values when not present on the request builder.
                builder = new OffsetFetchRequest.Builder(
                        groupId,
                        true,
                        new ArrayList<>(this.requestedPartitions),
                        throwOnFetchStableOffsetUnsupported);
            }
            return new NetworkClientDelegate.UnsentRequest(builder, coordinatorRequestManager.coordinator())
                    .whenComplete((r, t) -> onResponse(r.receivedTimeMs(), (OffsetFetchResponse) r.responseBody()));
        }

        /**
         * Handle request responses, including successful and failed.
         */
        public void onResponse(
                final long currentTimeMs,
                final OffsetFetchResponse response) {
            Errors responseError = response.groupLevelError(groupId);
            if (responseError != Errors.NONE) {
                onFailure(currentTimeMs, responseError);
                return;
            }
            onSuccess(currentTimeMs, response);
        }

        /**
         * Handle failed responses. This will retry if the error is retriable, or complete the
         * result future exceptionally in the case of non-recoverable or unexpected errors.
         */
        private void onFailure(final long currentTimeMs,
                               final Errors responseError) {
            handleCoordinatorDisconnect(responseError.exception(), currentTimeMs);
            log.debug("Offset fetch failed: {}", responseError.message());
            if (responseError == COORDINATOR_LOAD_IN_PROGRESS) {
                maybeRetry(currentTimeMs, responseError.exception());
            } else if (responseError == Errors.UNKNOWN_MEMBER_ID) {
                log.error("OffsetFetch failed with {} because the member is not part of the group" +
                    " anymore.", responseError);
                future.completeExceptionally(responseError.exception());
            } else if (responseError == Errors.STALE_MEMBER_EPOCH) {
                if (maybeRetryWithNewMemberEpoch(currentTimeMs, responseError)) {
                    log.debug("OffsetFetch failed with {} but the consumer is still part" +
                        " of the group, so the request will be retried with the latest " +
                        "member ID and epoch.", responseError);
                    return;
                }
                log.error("OffsetFetch failed with {} and the consumer is not part " +
                    "of the group anymore (it probably left the group, got fenced" +
                    " or failed). The request cannot be retried and will fail.", responseError);
                future.completeExceptionally(responseError.exception());
            } else if (responseError == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry
                coordinatorRequestManager.markCoordinatorUnknown("error response " + responseError.name(), currentTimeMs);
                maybeRetry(currentTimeMs, responseError.exception());
            } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
            } else {
                future.completeExceptionally(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
            }
        }

        /**
         * Enqueue the request to be retried with exponential backoff if the time has not expired.
         * This will fail the request future if the request is not retried.
         */
        @Override
        void maybeRetry(long currentTimeMs, Throwable throwable) {
            if (isExpired(currentTimeMs)) {
                future.completeExceptionally(throwable);
                return;
            }
            onFailedAttempt(currentTimeMs);
            pendingRequests.inflightOffsetFetches.remove(this);
            pendingRequests.addOffsetFetchRequest(this);
        }

        private boolean isExpired(final long currentTimeMs) {
            return expirationTimeMs <= currentTimeMs;
        }

        /**
         * Complete the request future with a TimeoutException if the request expired. No action
         * taken if the request is still active.
         *
         * @return True if the request expired.
         */
        private boolean maybeExpire(final long currentTimeMs) {
            if (isExpired(currentTimeMs)) {
                future.completeExceptionally(new TimeoutException("OffsetFetch request could not " +
                    "complete before timeout expired."));
                return true;
            }
            return false;
        }

        private void onSuccess(final long currentTimeMs,
                               final OffsetFetchResponse response) {
            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                    response.partitionDataMap(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.completeExceptionally(new KafkaException("Topic or Partition " + tp + " does " +
                                "not " +
                                "exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.completeExceptionally(new KafkaException("Unexpected error in fetch offset " +
                                "response for partition " + tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // TODO: Optimization question: Do we need to retry all partitions upon a single partition error?
                log.info("The following partitions still have unstable offsets " +
                        "which are not cleared on the broker side: {}" +
                        ", this could be either " +
                        "transactional offsets waiting for completion, or " +
                        "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                maybeRetry(currentTimeMs, Errors.UNSTABLE_OFFSET_COMMIT.exception());
            } else {
                future.complete(offsets);
            }
        }

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> chainFuture(
                final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> otherFuture) {
            return this.future.whenComplete((r, t) -> {
                if (t != null) {
                    otherFuture.completeExceptionally(t);
                } else {
                    otherFuture.complete(r);
                }
            });
        }

        @Override
        public String toString() {
            return "OffsetFetchRequestState{" +
                    "requestedPartitions=" + requestedPartitions +
                    ", memberId=" + memberInfo.memberId.orElse("undefined") +
                    ", memberEpoch=" + (memberInfo.memberEpoch.isPresent() ? memberInfo.memberEpoch.get() : "undefined") +
                    ", future=" + future +
                    ", " + toStringBase() +
                    '}';
        }
    }

    /**
     * <p>This is used to stage the unsent {@link OffsetCommitRequestState} and {@link OffsetFetchRequestState}.
     * <li>unsentOffsetCommits holds the offset commit requests that have not been sent out</>
     * <li>unsentOffsetFetches holds the offset fetch requests that have not been sent out</li>
     * <li>inflightOffsetFetches holds the offset fetch requests that have been sent out but not completed</>.
     * <p>
     * {@code addOffsetFetchRequest} dedupes the requests to avoid sending the same requests.
     */

    class PendingRequests {
        // Queue is used to ensure the sequence of commit
        Queue<OffsetCommitRequestState> unsentOffsetCommits = new LinkedList<>();
        List<OffsetFetchRequestState> unsentOffsetFetches = new ArrayList<>();
        List<OffsetFetchRequestState> inflightOffsetFetches = new ArrayList<>();

        // Visible for testing
        boolean hasUnsentRequests() {
            return !unsentOffsetCommits.isEmpty() || !unsentOffsetFetches.isEmpty();
        }

        OffsetCommitRequestState addOffsetCommitRequest(
            final Map<TopicPartition, OffsetAndMetadata> offsets,
            final Optional<Long> expirationTimeMs,
            final boolean retryOnStaleEpoch) {
            // TODO: Dedupe committing the same offsets to the same partitions
            OffsetCommitRequestState requestState = createOffsetCommitRequest(
                offsets,
                jitter,
                expirationTimeMs,
                retryOnStaleEpoch);
            return addOffsetCommitRequest(requestState);
        }

        OffsetCommitRequestState addOffsetCommitRequest(OffsetCommitRequestState request) {
            log.debug("Enqueuing OffsetCommit request for offsets: {}", request.offsets);
            unsentOffsetCommits.add(request);
            return request;
        }

        OffsetCommitRequestState createOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                           final OptionalDouble jitter,
                                                           final Optional<Long> expirationTimeMs,
                                                           final boolean retryOnStaleEpoch) {
            return jitter.isPresent() ?
                new OffsetCommitRequestState(
                    offsets,
                    groupId,
                    groupInstanceId,
                    expirationTimeMs,
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    jitter.getAsDouble(),
                    memberInfo,
                    retryOnStaleEpoch) :
                new OffsetCommitRequestState(
                    offsets,
                    groupId,
                    groupInstanceId,
                    expirationTimeMs,
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    memberInfo,
                    retryOnStaleEpoch);
        }

        /**
         * <p>Adding an offset fetch request to the outgoing buffer.  If the same request was made, we chain the future
         * to the existing one.
         *
         * <p>If the request is new, it invokes a callback to remove itself from the {@code inflightOffsetFetches}
         * upon completion.
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final OffsetFetchRequestState request) {
            Optional<OffsetFetchRequestState> dupe =
                    unsentOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();
            Optional<OffsetFetchRequestState> inflight =
                    inflightOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();

            if (dupe.isPresent() || inflight.isPresent()) {
                log.info("Duplicated OffsetFetchRequest: " + request.requestedPartitions);
                dupe.orElseGet(() -> inflight.get()).chainFuture(request.future);
            } else {
                // remove the request from the outbound buffer: inflightOffsetFetches
                request.future.whenComplete((r, t) -> {
                    if (!inflightOffsetFetches.remove(request)) {
                        log.warn("A duplicated, inflight, request was identified, but unable to find it in the " +
                                "outbound buffer:" + request);
                    }
                });
                this.unsentOffsetFetches.add(request);
            }
            return request.future;
        }

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final Set<TopicPartition> partitions,
                                                                                                final long expirationTimeMs) {
            OffsetFetchRequestState request = jitter.isPresent() ?
                    new OffsetFetchRequestState(
                            partitions,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            expirationTimeMs,
                            jitter.getAsDouble(),
                            memberInfo) :
                    new OffsetFetchRequestState(
                            partitions,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            expirationTimeMs,
                            memberInfo);
            return addOffsetFetchRequest(request);
        }

        /**
         * Clear {@code unsentOffsetCommits} and moves all the sendable request in {@code
         * unsentOffsetFetches} to the {@code inflightOffsetFetches} to bookkeep all the inflight
         * requests. Note: Sendable requests are determined by their timer as we are expecting
         * backoff on failed attempt. See {@link RequestState}.
         */
        List<NetworkClientDelegate.UnsentRequest> drain(final long currentTimeMs) {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

            // not ready to sent request
            List<OffsetCommitRequestState> unreadyCommitRequests = unsentOffsetCommits.stream()
                .filter(request -> !request.canSendRequest(currentTimeMs))
                .collect(Collectors.toList());

            failAndRemoveExpiredCommitRequests(currentTimeMs);

            // Add all unsent offset commit requests to the unsentRequests list
            unsentRequests.addAll(
                    unsentOffsetCommits.stream()
                        .filter(request -> request.canSendRequest(currentTimeMs))
                        .peek(request -> request.onSendAttempt(currentTimeMs))
                        .map(OffsetCommitRequestState::toUnsentRequest)
                        .collect(Collectors.toList()));

            // Partition the unsent offset fetch requests into sendable and non-sendable lists
            Map<Boolean, List<OffsetFetchRequestState>> partitionedBySendability =
                    unsentOffsetFetches.stream()
                            .collect(Collectors.partitioningBy(request -> request.canSendRequest(currentTimeMs)));

            failAndRemoveExpiredFetchRequests(currentTimeMs);

            // Add all sendable offset fetch requests to the unsentRequests list and to the inflightOffsetFetches list
            for (OffsetFetchRequestState request : partitionedBySendability.get(true)) {
                request.onSendAttempt(currentTimeMs);
                unsentRequests.add(request.toUnsentRequest());
                inflightOffsetFetches.add(request);
            }

            // Clear the unsent offset commit and fetch lists and add all non-sendable offset fetch requests to the unsentOffsetFetches list
            clearAll();
            unsentOffsetFetches.addAll(partitionedBySendability.get(false));
            unsentOffsetCommits.addAll(unreadyCommitRequests);

            return Collections.unmodifiableList(unsentRequests);
        }

        /**
         * Find the unsent commit requests that have expired, remove them and complete their
         * futures with a TimeoutException.
         */
        private void failAndRemoveExpiredCommitRequests(final long currentTimeMs) {
            unsentOffsetCommits.removeIf(req -> req.maybeExpire(currentTimeMs));
        }

        /**
         * Find the unsent fetch requests that have expired, remove them and complete their
         * futures with a TimeoutException.
         */
        private void failAndRemoveExpiredFetchRequests(final long currentTimeMs) {
            unsentOffsetFetches.removeIf(req -> req.maybeExpire(currentTimeMs));
        }

        private void clearAll() {
            unsentOffsetCommits.clear();
            unsentOffsetFetches.clear();
        }

        private List<NetworkClientDelegate.UnsentRequest> drainPendingCommits() {
            ArrayList<NetworkClientDelegate.UnsentRequest> res = new ArrayList<>();
            res.addAll(unsentOffsetCommits.stream().map(OffsetCommitRequestState::toUnsentRequest).collect(Collectors.toList()));
            clearAll();
            return res;
        }
    }

    /**
     * Encapsulates the state of auto-committing and manages the auto-commit timer.
     */
    private static class AutoCommitState {
        private final Timer timer;
        private final long autoCommitInterval;
        private boolean hasInflightCommit;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
            this.hasInflightCommit = false;
        }

        public boolean shouldAutoCommit() {
            return !this.hasInflightCommit && this.timer.isExpired();
        }

        public void resetTimer() {
            this.timer.reset(autoCommitInterval);
        }

        public void resetTimer(long retryBackoffMs) {
            this.timer.reset(retryBackoffMs);
        }

        public long remainingMs(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
            return this.timer.remainingMs();
        }

        public void updateTimer(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
        }

        public void setInflightCommitStatus(final boolean inflightCommitStatus) {
            this.hasInflightCommit = inflightCommitStatus;
        }
    }

    static class MemberInfo {
        Optional<String> memberId;
        Optional<Integer> memberEpoch;

        MemberInfo() {
            this.memberId = Optional.empty();
            this.memberEpoch = Optional.empty();
        }
    }
}
