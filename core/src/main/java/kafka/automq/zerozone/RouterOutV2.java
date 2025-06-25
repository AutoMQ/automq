package kafka.automq.zerozone;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import kafka.automq.interceptor.ProduceRequestArgs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;

public class RouterOutV2 {
    private final RouterChannel routerChannel;
    private final GetRouterOutNode mapping;
    private final Map<Node, Proxy> proxies = new ConcurrentHashMap<>();
    private final Time time;

    public RouterOutV2(RouterChannel routerChannel, GetRouterOutNode mapping, Time time) {
        this.routerChannel = routerChannel;
        this.mapping = mapping;
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
            CompletableFuture<RouterChannel.AppendResult> channelCf = routerChannel.append(node.id(), zoneRouterProduceRequest(args, flag, tp, records));
            CompletableFuture<Void> proxyCf = channelCf.thenCompose(channelRst -> {
                ProxyRequest proxyRequest = new ProxyRequest(tp, channelRst.epoch(), channelRst.channelOffset(), timeoutMillis);
                sendProxyRequest(node, proxyRequest);
                return proxyRequest.cf.thenAccept(response -> responseMap.put(tp, response));
            });
            cfList[i] = proxyCf;
            i++;
        }
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback = args.responseCallback();
        CompletableFuture.allOf(cfList).thenAccept(nil -> responseCallback.accept(responseMap));
    }

    private void sendProxyRequest(Node node, ProxyRequest proxyRequest) {
        Proxy proxy = proxies.computeIfAbsent(node, Proxy::new);
        proxy.send(proxyRequest);
    }

    class Proxy {
        private static final int MAX_BATCH_REQUEST_COUNT = 4096;
        private final Node node;
        private final Queue<ProxyRequest> waitingRequests = new ArrayBlockingQueue<>(1 << 15);

        public Proxy(Node node) {
            this.node = node;
        }

        public void send(ProxyRequest request) {

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

}
