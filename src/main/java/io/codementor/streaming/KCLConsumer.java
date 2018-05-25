package io.codementor.streaming;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import io.codementor.streaming.kcl.MetricsProcessorFactory;

public class KCLConsumer {
    public static void main(String[] args) {
        // Configuration for a worker instance
        final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                // Name of our application
                "metrics-processor",
                // Name of stream to process
                KinesisUtils.STREAM_NAME,
                new DefaultAWSCredentialsProviderChain(),
                // Name of this KCL worker instance. Should be different for different processes/machines
                "worker-1"
        );

        // Create a factory that knows how to create an instance of our records processor
        final IRecordProcessorFactory recordProcessorFactory = new MetricsProcessorFactory();
        // Create a KCL worker. We only need one per machine
        final Worker worker = new Worker.Builder()
                .config(config)
                .recordProcessorFactory(recordProcessorFactory)
                .build();

        // Start KCL worker
        worker.run();
    }
}
