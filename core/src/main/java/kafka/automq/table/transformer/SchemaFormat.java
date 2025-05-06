package kafka.automq.table.transformer;

public enum SchemaFormat {
    AVRO,
    JSON,
    PROTOBUF;

    public static SchemaFormat fromString(String format) {
        return switch (format) {
            case "AVRO" -> AVRO;
            case "JSON" -> JSON;
            case "PROTOBUF" -> PROTOBUF;
            default -> throw new IllegalArgumentException("Unsupported schema format: " + format);
        };
    }
}
