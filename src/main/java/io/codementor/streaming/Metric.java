package io.codementor.streaming;

/**
 * A class that represent a single metric value. The producer application
 * is writing serialized instances of this class to a Kinesis stream
 * and producers are reading stored records and deserialize them into
 * instances of this class.
 *
 * Every metric is a pair of a metric name and a double value.
 *
 * In a real application this class would be more complicated and could
 * additional information such as timestamp, host, aggregated statistics, etc.
 */
public class Metric {
    private String metricName;
    private double value;

    // Used by Jackson library during deserialization
    public Metric() {

    }

    public Metric(String metricName, double value) {
        this.metricName = metricName;
        this.value = value;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "metricName='" + metricName + '\'' +
                ", value=" + value +
                '}';
    }
}
