package kafka.log.streamaspect.log;

public interface ScanOffsetHolder {
    void flush();

    long getOffset(String name);

    void saveOffset(String name, long offset);
}
