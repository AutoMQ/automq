package kafka.autobalancer.model.samples;

public class SingleValueSamples implements Samples {
    private double value;

    @Override
    public void append(double value, long timestamp) {
        this.value = value;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}
