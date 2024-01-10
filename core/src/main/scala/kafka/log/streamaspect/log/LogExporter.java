package kafka.log.streamaspect.log;

public interface LogExporter {
    void export(String name, byte[] value);

    void flush();

    void close();
}
