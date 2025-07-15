package kafka.automq.zerozone;

import com.automq.stream.s3.cache.SnapshotReadCache;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DefaultReplayer implements Replayer {
    @Override
    public CompletableFuture<Void> replay(List<S3ObjectMetadata> objects) {
        return SnapshotReadCache.instance().replay(objects);
    }

    @Override
    public CompletableFuture<Void> replay(WriteAheadLog confirmWAL, long startOffset, long endOffset) {
        return SnapshotReadCache.instance().replay(confirmWAL, startOffset, endOffset);
    }
}
