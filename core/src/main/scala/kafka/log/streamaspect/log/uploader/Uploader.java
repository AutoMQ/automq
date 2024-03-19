package kafka.log.streamaspect.log.uploader;

/**
 * @author ipsum-0320
 */
public interface Uploader {
    String PREFIX = "logs/";
    String POSTFIX = ".log";
    String SHUTDOWN_SIGNAL = "shutdown-signal";
    int MAX_RETRY = 10;
    String BUFFER_FILE_NAME = "buffer-file.log";
    int BUFFER_SIZE = 16 * 1024 * 1024;

    void toBuffer(byte[] value);

    void upload(String key);

    void close();

    String wrapKey(String key);

}
