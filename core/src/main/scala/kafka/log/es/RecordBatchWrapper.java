package kafka.log.es;

import sdk.elastic.stream.api.KeyValue;
import sdk.elastic.stream.api.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class RecordBatchWrapper implements RecordBatch {
    private org.apache.kafka.common.record.RecordBatch inner;

    public RecordBatchWrapper(
            org.apache.kafka.common.record.RecordBatch inner) {
        this.inner = inner;
    }

    @Override
    public int count() {
        return inner.countOrNull();
    }

    @Override
    public long baseTimestamp() {
        // TODO: fix record batch
        return inner.maxTimestamp();
    }

    @Override
    public List<KeyValue> properties() {
        return Collections.emptyList();
    }

    @Override
    public ByteBuffer rawPayload() {
        // TODO: expose kafka under Bytebuf slice
        ByteBuffer buf = ByteBuffer.allocate(inner.sizeInBytes());
        inner.writeTo(buf);
        buf.flip();
        return buf;
    }
}
