package kafka.automq.zerozone;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Replayer {
    CompletableFuture<Void> replay(List<S3ObjectMetadata> objects);

    CompletableFuture<Void> replay(WriteAheadLog confirmWAL, RecordOffset startOffset, RecordOffset endOffset);

}
