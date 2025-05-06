package kafka.automq.table.transformer;

public class InvalidDataException extends RuntimeException {
    private static final long serialVersionUID = 4029025366392702726L;

    public InvalidDataException() {
    }

    public InvalidDataException(String msg) {
        super(msg);
    }

    public InvalidDataException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public InvalidDataException(Throwable cause) {
        super(cause);
    }
}
