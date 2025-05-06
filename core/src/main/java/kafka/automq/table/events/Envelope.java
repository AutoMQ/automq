package kafka.automq.table.events;

public class Envelope {
    private final int partition;
    private final long offset;
    private final Event event;

    public Envelope(int partition, long offset, Event event) {
        this.partition = partition;
        this.offset = offset;
        this.event = event;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Event event() {
        return event;
    }

}
