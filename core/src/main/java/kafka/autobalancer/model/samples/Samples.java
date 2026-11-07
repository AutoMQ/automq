package kafka.autobalancer.model.samples;

public interface Samples {
    void append(double value, long timestamp);

    double value();

    boolean isTrusted();
}
