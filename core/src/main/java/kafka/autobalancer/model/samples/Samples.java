package kafka.autobalancer.model.samples;

public interface Samples {
    void append(double value);

    double value();

    boolean isTrusted();
}
