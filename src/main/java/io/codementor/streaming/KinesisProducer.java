package io.codementor.streaming;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Random;

import static io.codementor.streaming.Utils.STREAM_NAME;
import static io.codementor.streaming.Utils.sleep;
import static io.codementor.streaming.Utils.toBytes;

/**
 * Writes metrics to a Kinesis stream.
 */
public class KinesisProducer {

    // In a real application we would collect metrics from multiple
    // machines and write them to the same Kinesis stream.
    // For demo purposes we emulate multiple hosts writing
    // to the same stream by using several threads.
    private static final int NUM_OF_HOSTS = 4;

    public static void main(String[] args) {

        for (int i = 0; i < NUM_OF_HOSTS; i++) {
            // Set a "host" name
            String hostName = "host-" + i;
            System.out.println("Launching producer for host: " + hostName);
            // Create a thread for writing data into a stream
            Thread thread = new Thread(new MetricsProducer(hostName));
            thread.start();
        }
    }
}

/**
 * A single producer thread
 */
class MetricsProducer implements Runnable {

    // Used to get current OS metrics
    private static OperatingSystemMXBean OS_BEAN = ManagementFactory.getOperatingSystemMXBean();

    private final String hostName;

    private final Random random = new Random();

    public MetricsProducer(String hostName) {
        this.hostName = hostName;
    }

    @Override
    public void run() {
        AmazonKinesis client = Utils.createKinesisClient();

        // Store metrics in an infinite loop
        while (true) {
            // Get current CPU load metric
            Metric metric = getCpuLoadMetric();

            PutRecordRequest putRecordRequest = new PutRecordRequest();
            // Set a stream name. The same as we created in previous example
            putRecordRequest.setStreamName(STREAM_NAME);
            // Set metric name as a key of the new record
            // Not all metrics will be sent to the same shard
            // since metrics from different "hosts" will have keys
            putRecordRequest.setPartitionKey(metric.getMetricName());
            // Finally we specify data that we want to store
            putRecordRequest.setData(toBytes(metric));

            try {
                // Store a record with a single metric value
                client.putRecord(putRecordRequest);
            } catch (AmazonClientException ex) {
                System.out.println("Failed to write Kinesis record");
                ex.printStackTrace();
            }
            sleep(1000);
        }
    }

    private Metric getCpuLoadMetric() {
        // Get system load metric and change it randomly for different processors
        // to avoid writing same value for different metrics
        double load = OS_BEAN.getSystemLoadAverage() + random.nextDouble();
        // Create a metric name
        String metricName = hostName + "-system-load";
        System.out.println(String.format("System load for %s: %f", metricName, load));

        // Create a metric instance
        return new Metric(metricName, load);
    }
}
