package kafka.automq.zerozone;

import kafka.automq.interceptor.ClientIdKey;
import kafka.automq.interceptor.ClientIdMetadata;
import kafka.automq.interceptor.ProduceRequestArgs;
import kafka.server.RequestLocal;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;

import com.automq.stream.utils.Systems;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

public class RouterInV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterInV2.class);
    private final RouterChannelProvider channelProvider;
    private final ElasticKafkaApis kafkaApis;
    private final String rack;
    private RouterInProduceHandler routerInProduceHandler;
    private final Queue<PartitionProduceRequest> unpackLinkQueue = new ConcurrentLinkedQueue<>();
    private final EventLoop[] appendEventLoops;
    private final FastThreadLocal<RequestLocal> requestLocals = new FastThreadLocal<>() {
        @Override
        protected RequestLocal initialValue() {
            // The same as KafkaRequestHandler.requestLocal
            return RequestLocal.withThreadConfinedCaching();
        }
    };

    public RouterInV2(RouterChannelProvider channelProvider, ElasticKafkaApis kafkaApis, String rack) {
        this.channelProvider = channelProvider;
        this.kafkaApis = kafkaApis;
        this.rack = rack;
        this.routerInProduceHandler = kafkaApis::handleProduceAppendJavaCompatible;

        this.appendEventLoops = new EventLoop[Systems.CPU_CORES];
        for (int i = 0; i < appendEventLoops.length; i++) {
            this.appendEventLoops[i] = new EventLoop("ROUTER_IN_V2_APPEND_" + i);
        }
    }


    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(AutomqZoneRouterRequestData request) {
        RouterRecordV2 routerRecord = RouterRecordV2.decode(Unpooled.wrappedBuffer(request.metadata()));
        RouterChannel routerChannel = channelProvider.readOnlyChannel(routerRecord.nodeId());
        List<CompletableFuture<AutomqZoneRouterResponseData.Response>> subResponseList = new ArrayList<>(routerRecord.channelOffsets().size());
        for (ByteBuf channelOffset : routerRecord.channelOffsets()) {
            PartitionProduceRequest partitionProduceRequest = new PartitionProduceRequest(ChannelOffset.of(channelOffset));
            partitionProduceRequest.unpackLinkCf = routerChannel.get(channelOffset);
            unpackLinkQueue.add(partitionProduceRequest);
            partitionProduceRequest.unpackLinkCf.thenAccept(nil -> handleUnpackLink());
            subResponseList.add(partitionProduceRequest.responseCf);
        }
        return CompletableFuture.allOf(subResponseList.toArray(new CompletableFuture[0])).thenApply(nil -> {
            AutomqZoneRouterResponseData response = new AutomqZoneRouterResponseData();
            response.setResponses(subResponseList.stream().map(CompletableFuture::join).collect(Collectors.toList()));
            return new AutomqZoneRouterResponse(response);
        });
    }

    private void handleUnpackLink() {
        if (unpackLinkQueue.isEmpty()) {
            return;
        }
        synchronized (unpackLinkQueue) {
            while (!unpackLinkQueue.isEmpty()) {
                PartitionProduceRequest req = unpackLinkQueue.peek();
                if (req.unpackLinkCf.isDone()) {
                    EventLoop eventLoop = appendEventLoops[Math.abs(req.channelOffset.orderHint() % appendEventLoops.length)];
                    req.unpackLinkCf.thenComposeAsync(buf -> {
                        try {
                            ZoneRouterProduceRequest zoneRouterProduceRequest = ZoneRouterPackReader.decodeDataBlock(buf).get(0);
                            return append(req.channelOffset, zoneRouterProduceRequest);
                        } finally {
                            buf.release();
                        }
                    }, eventLoop).whenComplete((resp, ex) -> {
                        if (ex != null) {
                            req.responseCf.completeExceptionally(ex);
                            return;
                        }
                        req.responseCf.complete(resp);
                    });
                    unpackLinkQueue.poll();
                } else {
                    break;
                }
            }
        }
    }

    private CompletableFuture<AutomqZoneRouterResponseData.Response> append(
        ChannelOffset channelOffset,
        ZoneRouterProduceRequest zoneRouterProduceRequest
    ) {
        ZoneRouterProduceRequest.Flag flag = new ZoneRouterProduceRequest.Flag(zoneRouterProduceRequest.flag());
        ProduceRequestData data = zoneRouterProduceRequest.data();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ROUTER_IN],[APPEND],data={}", data);
        }

        ZeroZoneThreadLocalContext.writeContext().channelOffset =  channelOffset;
        Map<TopicPartition, MemoryRecords> realEntriesPerPartition = ZeroZoneTrafficInterceptor.produceRequestToMap(data);
        short apiVersion = zoneRouterProduceRequest.apiVersion();
        CompletableFuture<AutomqZoneRouterResponseData.Response> cf = new CompletableFuture<>();
        routerInProduceHandler.handleProduceAppend(
            ProduceRequestArgs.builder()
                .clientId(buildClientId(realEntriesPerPartition))
                .timeout(data.timeoutMs())
                .requiredAcks(data.acks())
                .internalTopicsAllowed(flag.internalTopicsAllowed())
                .transactionId(data.transactionalId())
                .entriesPerPartition(realEntriesPerPartition)
                .responseCallback(rst -> {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[ROUTER_IN],[RESPONSE],response={}", rst);
                    }
                    @SuppressWarnings("deprecation")
                    ProduceResponse produceResponse = new ProduceResponse(rst, 0, Collections.emptyList());
                    AutomqZoneRouterResponseData.Response response = new AutomqZoneRouterResponseData.Response()
                        .setData(ZoneRouterResponseCodec.encode(produceResponse.data()).array());
                    cf.complete(response);
                })
                .recordValidationStatsCallback(rst -> {
                })
                .apiVersion(apiVersion)
                .requestLocal(requestLocals.get())
                .build()
        );
        return cf;
    }

    public void setRouterInProduceHandler(RouterInProduceHandler routerInProduceHandler) {
        this.routerInProduceHandler = routerInProduceHandler;
    }

    protected ClientIdMetadata buildClientId(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        String clientId = String.format("%s=%s", ClientIdKey.AVAILABILITY_ZONE, rack);
        String connectionId = getConnectionIdFrom(entriesPerPartition);
        return ClientIdMetadata.of(clientId, null, connectionId);
    }

    protected String getConnectionIdFrom(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        for (Map.Entry<TopicPartition, MemoryRecords> entry : entriesPerPartition.entrySet()) {
            for (MutableRecordBatch batch : entry.getValue().batches()) {
                if (batch.hasProducerId()) {
                    return String.valueOf(batch.producerId());
                }
            }
        }
        return null;
    }

    static class PartitionProduceRequest {
        final ChannelOffset channelOffset;
        CompletableFuture<ByteBuf> unpackLinkCf;
        final CompletableFuture<AutomqZoneRouterResponseData.Response> responseCf = new CompletableFuture<>();

        public PartitionProduceRequest(ChannelOffset channelOffset) {
            this.channelOffset = channelOffset;
        }
    }

}
