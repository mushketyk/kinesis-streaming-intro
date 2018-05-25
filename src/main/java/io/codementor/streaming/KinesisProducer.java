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

public class KinesisProducer {

    private static final int NUM_OF_HOSTS = 4;

    public static void main(String[] args) {

        for (int i = 0; i < NUM_OF_HOSTS; i++) {
            String hostName = "host-" + i;
            System.out.println("Launching producer for host: " + hostName);
            Thread thread = new Thread(new MetricsProducer(hostName));
            thread.start();
        }
    }
}

class MetricsProducer implements Runnable {
    
    private static OperatingSystemMXBean OS_BEAN = ManagementFactory.getOperatingSystemMXBean();

    private final String hostName;
    
    private final Random random = new Random();

    public MetricsProducer(String hostName) {
        this.hostName = hostName;
    }

    @Override
    public void run() {
        AmazonKinesis client = Utils.createKinesisClient();

        while (true) {
            Metric metric = getCpuLoadMetric();

            PutRecordRequest putRecordRequest = new PutRecordRequest();
            // Set a stream name. The same as we created in previous example
            putRecordRequest.setStreamName(STREAM_NAME);
            // Set metric name as a key of the new record
            putRecordRequest.setPartitionKey(metric.getMetricName());
            // Finally we specify data that we want to store
            putRecordRequest.setData(toBytes(metric));

            try {
                client.putRecord(putRecordRequest);
            } catch (AmazonClientException ex) {
                System.out.println("Failed to write Kinesis record");
                ex.printStackTrace();
            }
            sleep(1000);
        }
    }

    private Metric getCpuLoadMetric() {
        double load = OS_BEAN.getSystemLoadAverage() + random.nextDouble();
        String metricName = hostName + "-system-load";
        System.out.println(String.format("System load for %s: %f", metricName, load));

        return new Metric(metricName, load);
    }
}
