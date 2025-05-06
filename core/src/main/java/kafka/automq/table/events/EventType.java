package kafka.automq.table.events;

public enum EventType {
    COMMIT_REQUEST(0),
    COMMIT_RESPONSE(1);

    private final int id;

    EventType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static EventType fromId(int id) {
        switch (id) {
            case 0:
                return COMMIT_REQUEST;
            case 1:
                return COMMIT_RESPONSE;
            default:
                throw new IllegalArgumentException("Unknown event type id: " + id);
        }
    }
}
