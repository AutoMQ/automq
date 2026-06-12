/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.availability;

import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import kafka.automq.availability.action.AvailabilityActionAdapter;
import kafka.automq.availability.action.AvailabilityActionExecutor;
import kafka.automq.availability.action.NodeExitActionAdapter;
import kafka.automq.availability.action.SegmentRollActionAdapter;
import kafka.automq.availability.action.SkipReadRangeActionAdapter;
import kafka.automq.availability.action.SkippedReadRangeRegistry;
import kafka.automq.availability.broker.ActionResponsePublisher;
import kafka.automq.availability.broker.AvailabilitySignalPublisher;
import kafka.automq.availability.broker.BrokerActionReceiver;
import kafka.automq.availability.broker.BrokerAvailabilityService;
import kafka.automq.availability.transport.AvailabilityBrokerKvCodec;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;
import kafka.automq.availability.transport.NodeToControllerAvailabilityKvSender;
import kafka.server.BrokerLifecycleManager;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.NodeToControllerChannelManager;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds availability runtime services from server-owned dependencies while keeping server lifecycle code narrow.
 */
public final class AvailabilityRuntimeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvailabilityRuntimeFactory.class);

    private AvailabilityRuntimeFactory() {
    }

    public static BrokerAvailabilityService brokerService(KafkaConfig config,
                                                          Time time,
                                                          BrokerLifecycleManager lifecycleManager,
                                                          MetadataImageReader imageReader,
                                                          NodeToControllerChannelManager channelManager,
                                                          SegmentRollActionAdapter.SegmentRoller segmentRoller,
                                                          SkipReadRangeActionAdapter.SegmentExclusiveEndOffsetResolver
                                                              segmentEndOffsetResolver) {
        AvailabilityBrokerKvCodec codec = new AvailabilityBrokerKvCodec();
        AvailabilityKvRequestSender sender = new NodeToControllerAvailabilityKvSender(channelManager);
        BrokerAvailabilityMonitor monitor = new BrokerAvailabilityMonitor(config.nodeId(), lifecycleManager.brokerEpoch(),
            config.automqAutoFallbackAppendStuckThresholdMs() * 1_000_000L,
            config.automqAutoFallbackColdReadStuckThresholdMs() * 1_000_000L,
            config.automqAutoFallbackPartitionCloseStuckThresholdMs() * 1_000_000L,
            config.automqAutoFallbackLogReadFailThreshold(), time::nanoseconds);
        AvailabilitySignalPublisher signalPublisher = new AvailabilitySignalPublisher(monitor, sender, time);
        ActionResponsePublisher responsePublisher = new ActionResponsePublisher(sender);
        BrokerActionReceiver receiver = new BrokerActionReceiver(config.nodeId());
        AvailabilityActionExecutor actionExecutor = new AvailabilityActionExecutor(brokerAdapters(segmentRoller,
            segmentEndOffsetResolver), time);
        ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("availability-broker-runtime-%d", true), LOGGER);
        return new BrokerAvailabilityService(monitor, signalPublisher, receiver, responsePublisher,
            () -> imageReader.read(image -> codec.collectActions(image.kv())), actionExecutor, sender, scheduler, time,
            config.automqAutoFallbackSignalWriteIntervalMs(), config.automqAutoFallbackSignalWriteIntervalMs());
    }

    public interface MetadataImageReader {
        <T> T read(Function<MetadataImage, T> reader);
    }

    private static Map<AvailabilityActionType, AvailabilityActionAdapter> brokerAdapters(
        SegmentRollActionAdapter.SegmentRoller segmentRoller,
        SkipReadRangeActionAdapter.SegmentExclusiveEndOffsetResolver segmentEndOffsetResolver) {
        Map<AvailabilityActionType, AvailabilityActionAdapter> adapters = new EnumMap<>(AvailabilityActionType.class);
        SkippedReadRangeRegistry skippedReadRanges = new SkippedReadRangeRegistry();
        AvailabilityRuntimeHooks.registerSkippedReadRanges(skippedReadRanges);
        adapters.put(AvailabilityActionType.NODE_EXIT, new NodeExitActionAdapter());
        adapters.put(AvailabilityActionType.SEGMENT_ROLL, new SegmentRollActionAdapter(segmentRoller));
        adapters.put(AvailabilityActionType.SKIP_READ_RANGE,
            new SkipReadRangeActionAdapter(skippedReadRanges, segmentEndOffsetResolver));
        return adapters;
    }

}
