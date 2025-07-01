package kafka.automq.zerozone;

import kafka.automq.interceptor.ProduceRequestArgs;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterRequest;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RouterOutV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterOutV2.class);
    private final Node currentNode;
    private final RouterChannel routerChannel;
    private final Map<Node, Proxy> proxies = new ConcurrentHashMap<>();
    private final EventLoop eventLoop = new EventLoop("ROUTER_OUT_V2");

    private final GetRouterOutNode mapping;
    private final ElasticKafkaApis kafkaApis;
    private final AsyncSender asyncSender;
    private final Time time;
    private final NetworkBandwidthLimiter inboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND);


    public RouterOutV2(Node currentNode, RouterChannel routerChannel, GetRouterOutNode mapping,
        ElasticKafkaApis kafkaApis, AsyncSender asyncSender, Time time) {
        this.currentNode = currentNode;
        this.routerChannel = routerChannel;
        this.mapping = mapping;
        this.kafkaApis = kafkaApis;
        this.asyncSender = asyncSender;
        this.time = time;
    }

    public void handleProduceAppendProxy(ProduceRequestArgs args) {
        long timeoutMillis = time.milliseconds() + args.timeout();
        short flag = new ZoneRouterProduceRequest.Flag().internalTopicsAllowed(args.internalTopicsAllowed()).value();
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new ConcurrentHashMap<>();
        @SuppressWarnings("rawtypes") CompletableFuture[] cfList = new CompletableFuture[args.entriesPerPartition().size()];
        int i = 0;
        for (Map.Entry<TopicPartition, MemoryRecords> entry : args.entriesPerPartition().entrySet()) {
            TopicPartition tp = entry.getKey();
            MemoryRecords records = entry.getValue();
            Node node = mapping.getRouteOutNode(tp.topic(), tp.partition(), args.clientId());
            if (node.id() != currentNode.id()) {
                inboundLimiter.consume(ThrottleStrategy.BYPASS, records.buffer().remaining());
            }
            if (node.id() == Node.noNode().id()) {
                responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER));
                continue;
            }
            int orderHint = orderHint(tp, args.clientId().connectionId());
            CompletableFuture<RouterChannel.AppendResult> channelCf = routerChannel.append(node.id(), orderHint, zoneRouterProduceRequest(args, flag, tp, records));
            CompletableFuture<Void> proxyCf = channelCf.thenCompose(channelRst -> {
                ProxyRequest proxyRequest = new ProxyRequest(tp, channelRst.epoch(), channelRst.channelOffset(), timeoutMillis);
                sendProxyRequest(node, proxyRequest);
                return proxyRequest.cf.thenAccept(response -> responseMap.put(tp, response));
            });
            cfList[i] = proxyCf;
            i++;
        }
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback = args.responseCallback();
        CompletableFuture.allOf(cfList).thenAccept(nil -> responseCallback.accept(responseMap)).exceptionally(ex -> {
            LOGGER.error("[UNEXPECTED],[ROUTE_FAIL]", ex);
            return null;
        });
    }

    private static int orderHint(TopicPartition tp, String connectionId) {
        return Objects.hash(tp.topic(), tp.partition(), connectionId);
    }

    private void sendProxyRequest(Node node, ProxyRequest proxyRequest) {
        Proxy proxy = proxies.computeIfAbsent(node, RemoteProxy::new);
        proxy.send(proxyRequest);
    }

    interface Proxy {
        void send(ProxyRequest request);
    }

    // TODO: cleanup expired proxy
    // TODO: proxy to local or special buffer for local to simple the logic, we can optimize the logic after implement RouteIn
    class RemoteProxy implements Proxy {
        private static final int MAX_BATCH_REQUEST_COUNT = 4096;
        private static final int MAX_INFLIGHT_SIZE = 4;
        private final Node node;
        private final BlockingQueue<ProxyRequest> waitingRequests = new ArrayBlockingQueue<>(1 << 15);
        private final List<ProxyRequest> reusableBatchSendList = new ArrayList<>(MAX_BATCH_REQUEST_COUNT);
        private final Semaphore inflightLimiter = new Semaphore(MAX_INFLIGHT_SIZE);

        public RemoteProxy(Node node) {
            this.node = node;
        }

        public void send(ProxyRequest request) {
            boolean success = waitingRequests.offer(request);
            if (!success) {
                // TODO: return 503
                LOGGER.error("[ROUTE_PROXY],[WAITING_FULL],node={},size={}", node, waitingRequests.size());
                return;
            }
            drainAndBatchSend();
        }

        private void drainAndBatchSend() {
            if (!inflightLimiter.tryAcquire()) {
                return;
            }
            synchronized (waitingRequests) {
                if (waitingRequests.isEmpty()) {
                    inflightLimiter.release();
                    return;
                }
                List<ProxyRequest> requests = reusableBatchSendList;
                requests.clear();
                waitingRequests.drainTo(requests);
                if (requests.isEmpty()) {
                    inflightLimiter.release();
                    return;
                }
                // TODO: batch request count
                // TODO: group by route epoch
                batchSend(requests).whenComplete((nil, ex) -> inflightLimiter.release());
            }

        }

        private CompletableFuture<Void> batchSend(List<ProxyRequest> requests) {
            RouterRecordV2 routerRecord = new RouterRecordV2(
                currentNode.id(),
                requests.stream().map(r -> r.channelOffset).collect(Collectors.toList())
            );
            AutomqZoneRouterRequest.Builder builder = new AutomqZoneRouterRequest.Builder(
                new AutomqZoneRouterRequestData().setMetadata(routerRecord.encode().array())
                    .setVersion((short) 1)
                    .setRouteEpoch(0)
            );
            return asyncSender.sendRequest(node, builder).thenAccept(clientResponse -> {
                if (!clientResponse.hasResponse()) {
                    LOGGER.error("[ROUTER_OUT],[NO_RESPONSE],response={}", clientResponse);
                    requests.forEach(ProxyRequest::completeWithUnknownError);
                    return;
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("[ROUTER_OUT],[RESPONSE],response={}", clientResponse);
                }
                AutomqZoneRouterResponse zoneRouterResponse = (AutomqZoneRouterResponse) clientResponse.responseBody();
                handleRouterResponse(zoneRouterResponse, requests);
            }).exceptionally(ex -> {
                LOGGER.error("[ROUTER_OUT],[REQUEST_FAIL]", ex);
                requests.forEach(ProxyRequest::completeWithUnknownError);
                return null;
            });
        }

        private void handleRouterResponse(AutomqZoneRouterResponse zoneRouterResponse, List<ProxyRequest> requests) {
            List<AutomqZoneRouterResponseData.Response> responses = zoneRouterResponse.data().responses();
            Iterator<ProxyRequest> requestIt = requests.iterator();
            Iterator<AutomqZoneRouterResponseData.Response> responseIt = responses.iterator();
            while (requestIt.hasNext()) {
                ProxyRequest request = requestIt.next();
                AutomqZoneRouterResponseData.Response response = responseIt.next();
                ProduceResponseData produceResponseData = ZoneRouterResponseCodec.decode(Unpooled.wrappedBuffer(response.data()));
                response.setData(null); // gc the data
                ProduceResponseData.PartitionProduceResponse partitionData = produceResponseData.responses().iterator().next()
                    .partitionResponses().get(0);
                request.cf.complete(partitionResponse(partitionData));
            }

        }

    }

    static class ProxyRequest {
        final TopicPartition topicPartition;
        long epoch;
        ByteBuf channelOffset;
        final long timeoutMillis;
        final CompletableFuture<ProduceResponse.PartitionResponse> cf = new CompletableFuture<>();

        public ProxyRequest(TopicPartition topicPartition, long epoch, ByteBuf channelOffset, long timeoutMillis) {
            this.topicPartition = topicPartition;
            this.epoch = epoch;
            this.channelOffset = channelOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public void completeWithUnknownError() {
            completeWithError(Errors.UNKNOWN_SERVER_ERROR);
        }

        private void completeWithError(Errors errors) {
            ProduceResponse.PartitionResponse rst = new ProduceResponse.PartitionResponse(errors, -1, -1, -1, -1, Collections.emptyList(), "");
            cf.complete(rst);
        }
    }

    private static ByteBuf zoneRouterProduceRequest(ProduceRequestArgs args, short flag, TopicPartition tp,
        MemoryRecords records) {
        ProduceRequestData data = new ProduceRequestData();
        data.setTransactionalId(args.transactionId());
        data.setAcks(args.requiredAcks());
        data.setTimeoutMs(args.timeout());
        ProduceRequestData.TopicProduceDataCollection topics = new ProduceRequestData.TopicProduceDataCollection();
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData();
        topic.setName(tp.topic());
        topic.setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(tp.partition()).setRecords(records)));
        topics.add(topic);
        data.setTopicData(topics);
        ZoneRouterProduceRequest request = new ZoneRouterProduceRequest(args.apiVersion(), flag, data);
        return ZoneRouterPackWriter.encodeDataBlock(List.of(request));
    }

    private static ProduceResponse.PartitionResponse partitionResponse(
        ProduceResponseData.PartitionProduceResponse partitionData) {
        return new ProduceResponse.PartitionResponse(
            Errors.forCode(partitionData.errorCode()),
            partitionData.baseOffset(),
            0, // last offset , the network layer don't need
            partitionData.logAppendTimeMs(),
            partitionData.logStartOffset(),
            partitionData.recordErrors().stream().map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage())).collect(Collectors.toList()),
            partitionData.errorMessage(),
            partitionData.currentLeader()
        );
    }

}
