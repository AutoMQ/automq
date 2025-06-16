package kafka.automq.zerozone;

import com.automq.stream.s3.wal.WriteAheadLog;

public interface ConfirmWALProvider {

    WriteAheadLog readOnly(String walConfig, int nodeId);

}
