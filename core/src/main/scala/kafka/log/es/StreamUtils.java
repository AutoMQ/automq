package kafka.log.es;

import sdk.elastic.stream.api.Stream;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class StreamUtils {

    public static Map<String, ByteBuffer> fetchKV(Stream stream, Set<String> keys) {
        // TODO: find KV from last to first
        return Collections.emptyMap();
    }

}
