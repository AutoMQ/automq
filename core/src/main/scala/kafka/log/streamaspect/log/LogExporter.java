package kafka.log.streamaspect.log;

public interface LogExporter {
    void export(String name, String value);

    void flush();

    void close();
}
