package io.codementor.streaming;

public class Metric {
    private String metricName;
    private double value;

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
