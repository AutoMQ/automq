package kafka.automq.table.worker;

import java.util.List;
import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import org.apache.kafka.server.record.TableTopicSchemaType;
import org.apache.kafka.storage.internals.log.LogConfig;

import static kafka.automq.table.utils.PartitionUtil.stringToList;

public class WorkerConfig {
    static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

    private final UnifiedLog log;
    private LogConfig config;

    public WorkerConfig(Partition partition) {
        this.log = partition.log().get();
        this.config = log.config();
    }

    // for testing
    public WorkerConfig() {
        this.log = null;
    }

    public String namespace() {
        return config.tableTopicNamespace;
    }

    public TableTopicSchemaType schemaType() {
        return config.tableTopicSchemaType;
    }

    public long incrementSyncThreshold() {
        return 32 * 1024 * 1024;
    }

    public int microSyncBatchSize() {
        return 32 * 1024 * 1024;
    }

    public List<String> idColumns() {
        String str = config.tableTopicIdColumns;
        return stringToList(str, COMMA_NO_PARENS_REGEX);
    }

    public String partitionByConfig() {
        return config.tableTopicPartitionBy;
    }

    public List<String> partitionBy() {
        String str = config.tableTopicPartitionBy;
        return stringToList(str, COMMA_NO_PARENS_REGEX);
    }

    public boolean upsertEnable() {
        return config.tableTopicUpsertEnable;
    }

    public String cdcField() {
        return config.tableTopicCdcField;
    }

    public void refresh() {
        this.config = log.config();
    }


}
