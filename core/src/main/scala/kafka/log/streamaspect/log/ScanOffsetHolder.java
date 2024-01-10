package kafka.log.streamaspect.log;

public interface ScanOffsetHolder {
    void flush(boolean shutdown);

    long getOffset(String name);

    long getStoreIndex();

    void setStoreIndex(long storeIndex);

    void saveOffset(String name, long offset);

    boolean isCleanShutdown();
}
