package kafka.automq.zerozone;

public interface RouterChannelProvider {

    RouterChannel routerChannel(int node);

}
