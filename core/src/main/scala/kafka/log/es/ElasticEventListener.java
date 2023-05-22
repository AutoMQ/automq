package kafka.log.es;

public interface ElasticEventListener {
    void onEvent(long id, ElasticMetaEvent event);
}
