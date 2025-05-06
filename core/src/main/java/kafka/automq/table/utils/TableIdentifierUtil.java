package kafka.automq.table.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.catalog.TableIdentifier;

public class TableIdentifierUtil {

    public static TableIdentifier of(String namespace, String name) {
        if (StringUtils.isBlank(namespace)) {
            return TableIdentifier.of(name);
        } else {
            return TableIdentifier.of(namespace, name);
        }
    }

}
