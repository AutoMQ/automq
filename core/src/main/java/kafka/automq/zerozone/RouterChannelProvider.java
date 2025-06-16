package kafka.automq.zerozone;

public interface RouterChannelProvider {

    RouterChannel channel();

    RouterChannel readOnlyChannel(int node);

}
