package kafka.log.streamaspect.log;

public interface LogExporter {
    boolean export(String name, String value);

    void flush();

    void close();
}
