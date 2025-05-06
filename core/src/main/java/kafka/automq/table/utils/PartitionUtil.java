package kafka.automq.table.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.expressions.Expressions.bucket;

public class PartitionUtil {
    private static final Pattern LIST_STRING_REGEX = Pattern.compile("\\[(.*)\\]");
    static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";
    private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

    public static List<String> parsePartitionBy(String str) {
        return stringToList(str, COMMA_NO_PARENS_REGEX);
    }

    public static PartitionSpec buildPartitionSpec(List<String> partitions, Schema schema) {
        if (partitions.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        partitions.forEach(
            partitionField -> {
                Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
                if (matcher.matches()) {
                    String transform = matcher.group(1);
                    switch (transform) {
                        case "year":
                            specBuilder.year(matcher.group(2));
                            break;
                        case "month":
                            specBuilder.month(matcher.group(2));
                            break;
                        case "day":
                            specBuilder.day(matcher.group(2));
                            break;
                        case "hour":
                            specBuilder.hour(matcher.group(2));
                            break;
                        case "bucket": {
                            Pair<String, Integer> args = transformArgPair(matcher.group(2));
                            specBuilder.bucket(args.first(), args.second());
                            break;
                        }
                        case "truncate": {
                            Pair<String, Integer> args = transformArgPair(matcher.group(2));
                            specBuilder.truncate(args.first(), args.second());
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException("Unsupported transform: " + transform);
                    }
                } else {
                    specBuilder.identity(partitionField);
                }
            });
        return specBuilder.build();
    }

    public static boolean evolve(List<String> newPartitions, Table table) {
        PartitionSpec spec = table.spec();
        if (newPartitions.isEmpty() && spec.isUnpartitioned()) {
            return false;
        }
        Map<Integer, PartitionField> id2field = new HashMap<>();
        Set<Integer> newPartitionFieldIdSet = new HashSet<>();
        spec.fields().forEach(f -> id2field.put(f.sourceId(), f));

        Schema tableSchema = table.schema();
        UpdatePartitionSpec updateSpec = table.updateSpec();
        int changeCount = 0;
        // add/replace partition
        for (String newPartition : newPartitions) {
            Matcher matcher = TRANSFORM_REGEX.matcher(newPartition);
            String transformer;
            String fieldName;
            if (matcher.matches()) {
                transformer = matcher.group(1);
                if ("bucket".equals(transformer) || "truncate".equals(transformer)) {
                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                    fieldName = args.first();
                } else {
                    fieldName = matcher.group(2);
                }
            } else {
                transformer = "identity";
                fieldName = newPartition;
            }
            Types.NestedField nestedField = tableSchema.findField(fieldName);
            if (nestedField == null) {
                continue;
            }
            newPartitionFieldIdSet.add(nestedField.fieldId());
            switch (transformer) {
                case "year": {
                    changeCount += addOrUpdate(nestedField, Expressions.year(fieldName), updateSpec, id2field);
                    break;
                }
                case "month": {
                    changeCount += addOrUpdate(nestedField, Expressions.month(fieldName), updateSpec, id2field);
                    break;
                }
                case "day": {
                    changeCount += addOrUpdate(nestedField, Expressions.day(fieldName), updateSpec, id2field);
                    break;
                }
                case "hour": {
                    changeCount += addOrUpdate(nestedField, Expressions.hour(fieldName), updateSpec, id2field);
                    break;
                }
                case "bucket": {
                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                    changeCount += addOrUpdate(nestedField, bucket(args.first(), args.second()), updateSpec, id2field);
                    break;
                }
                case "truncate": {
                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                    changeCount += addOrUpdate(nestedField, Expressions.truncate(args.first(), args.second()), updateSpec, id2field);
                    break;
                }
                case "identity": {
                    changeCount += addOrUpdate(nestedField, Expressions.ref(fieldName), updateSpec, id2field);
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + transformer);
            }
        }
        // drop partition
        for (PartitionField partitionField : spec.fields()) {
            Types.NestedField nestedField = tableSchema.findField(partitionField.sourceId());
            if (nestedField == null || !newPartitionFieldIdSet.contains(nestedField.fieldId())) {
                updateSpec.removeField(partitionField.name());
                changeCount++;
            }
        }
        if (changeCount > 0) {
            updateSpec.commit();
            return true;
        }
        return false;
    }

    private static int addOrUpdate(Types.NestedField nestedField, Term term, UpdatePartitionSpec updateSpec,
        Map<Integer, PartitionField> id2field) {
        PartitionField partitionField = id2field.get(nestedField.fieldId());
        if (partitionField != null) {
            if (term instanceof UnboundTransform<?, ?>) {
                //noinspection rawtypes
                if (((UnboundTransform) term).transform().equals(partitionField.transform())) {
                    return 0;
                }
            } else if (term instanceof NamedReference && Transforms.identity().equals(partitionField.transform())) {
                return 0;
            }
            updateSpec.removeField(partitionField.name());
        }
        updateSpec.addField(term);
        return 1;
    }

    private static Pair<String, Integer> transformArgPair(String argsStr) {
        String[] parts = argsStr.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
        }
        return Pair.of(parts[0].trim(), Integer.parseInt(parts[1].trim()));
    }

    public static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }
        Matcher matcher = LIST_STRING_REGEX.matcher(value);
        if (matcher.matches()) {
            value = matcher.group(1);
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
