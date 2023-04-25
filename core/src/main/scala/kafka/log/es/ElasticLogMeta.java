package kafka.log.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

// TODO: save meta to stream periodically. save meta when broker shutdown.
public class ElasticLogMeta {
    private List<ElasticStreamSegmentMeta> segments;
    private Long dataStreamId;
    private Long timeStreamId;
    private Long txnStreamId;

    public static ElasticLogMeta of(long dataStreamId, long timeStreamId, long txnStreamId) {
        ElasticLogMeta logMeta = new ElasticLogMeta();
        logMeta.dataStreamId = dataStreamId;
        logMeta.timeStreamId = timeStreamId;
        logMeta.txnStreamId = txnStreamId;
        logMeta.segments = new LinkedList<>();
        return logMeta;
    }

    public static ElasticLogMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.ISO_8859_1.decode(buf).toString();
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(metaStr, ElasticLogMeta.class);
        } catch (JsonProcessingException e) {
            // TODO: throw a better exception
            throw new RuntimeException(e);
        }
    }

    public List<ElasticStreamSegmentMeta> getSegments() {
        return segments;
    }

    public void setSegments(List<ElasticStreamSegmentMeta> segments) {
        this.segments = segments;
    }

    public Long getDataStreamId() {
        return dataStreamId;
    }

    public void setDataStreamId(Long dataStreamId) {
        this.dataStreamId = dataStreamId;
    }

    public Long getTimeStreamId() {
        return timeStreamId;
    }

    public void setTimeStreamId(Long timeStreamId) {
        this.timeStreamId = timeStreamId;
    }

    public Long getTxnStreamId() {
        return txnStreamId;
    }

    public void setTxnStreamId(Long txnStreamId) {
        this.txnStreamId = txnStreamId;
    }
}
