package kafka.log.es;

import sdk.elastic.stream.api.KeyValue;
import sdk.elastic.stream.api.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class SingleRecordBatch implements RecordBatch {
    private ByteBuffer payload;

    public SingleRecordBatch(ByteBuffer payload) {
        this.payload = payload;
    }

    @Override
    public int count() {
        return 1;
    }

    @Override
    public long baseTimestamp() {
        return 0;
    }

    @Override
    public List<KeyValue> properties() {
        return Collections.emptyList();
    }

    @Override
    public ByteBuffer rawPayload() {
        return payload.duplicate();
    }
}
