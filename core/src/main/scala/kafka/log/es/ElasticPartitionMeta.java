package kafka.log.es;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Partition dimension metadata, record the recover checkpoint, clean offset...
 */
public class ElasticPartitionMeta {
    @JsonProperty("r")
    private Long recoverOffset;
    @JsonProperty("c")
    private Long cleanerOffset;

    public static ByteBuffer encode(ElasticPartitionMeta meta) {
        ObjectMapper om = new ObjectMapper();
        try {
            String str = om.writeValueAsString(meta);
            return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static ElasticPartitionMeta decode(ByteBuffer buf) {
        String metaStr = StandardCharsets.UTF_8.decode(buf).toString();
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(metaStr, ElasticPartitionMeta.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Long getRecoverOffset() {
        return recoverOffset;
    }

    public void setRecoverOffset(Long recoverOffset) {
        this.recoverOffset = recoverOffset;
    }

    public Long getCleanerOffset() {
        return cleanerOffset;
    }

    public void setCleanerOffset(Long cleanerOffset) {
        this.cleanerOffset = cleanerOffset;
    }
}
