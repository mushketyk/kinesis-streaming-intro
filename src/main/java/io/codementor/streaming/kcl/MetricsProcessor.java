package io.codementor.streaming.kcl;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import io.codementor.streaming.Utils;
import io.codementor.streaming.Metric;

/**
 * Metrics processor for Kinesis Client Library.
 */
public class MetricsProcessor implements IRecordProcessor {
    @Override
    public void initialize(InitializationInput initializationInput) {

    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        // Iterate through a list of new records to process
        for (Record record : processRecordsInput.getRecords()) {
            // First we need to de-serialize a metric object
            Metric metric = parseMetric(record);
            // Now we can process a single record. Here we just print
            // record to a console, but we could also send a notification
            // to a third-party system, write data to a database, calculate
            // statistics, etc.
            System.out.println(metric);
        }
        // At last we need to signal that this record is processed
        // Once a record is processed it won't be send to our processor again
        // Here since we don't specify a record to checkout KCL will
        // checkout the last record in the input records
        checkpoint(processRecordsInput.getCheckpointer());
    }

    private Metric parseMetric(Record record) {
        return Utils.fromBytes(record.getData());
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (InvalidStateException|ShutdownException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        ShutdownReason reason = shutdownInput.getShutdownReason();
        switch (reason) {
            // Re-sharding, no more records in current shard
            case TERMINATE:
                // Application shutdown
            case REQUESTED:
                checkpoint(shutdownInput.getCheckpointer());
                break;

            // Processing will be moved to a different record processor
            case ZOMBIE:
                // No need to checkpoint
                break;
        }
    }
}
