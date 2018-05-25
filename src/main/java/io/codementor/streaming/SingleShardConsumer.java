package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

import static io.codementor.streaming.Utils.STREAM_NAME;
import static io.codementor.streaming.Utils.fromBytes;
import static io.codementor.streaming.Utils.sleep;

/**
 * A Kinesis consumer that is reading records from a *single* shard.
 * If you run this example you won't see all records if you have more than one
 * shard in your stream. This is because we need to read records from *all* shards
 * to process all records.
 */
public class SingleShardConsumer {

    // A name of a shard to read data from
    // List of shards in a stream can be displayed by the "GetListsOfShards" example
    public static final String SHARD_ID = "shardId-000000000000";

    public static void main(String[] args) {

        AmazonKinesis client = Utils.createKinesisClient();
        // Get iterator to start reading data from the Kinesis stream
        String shardIterator = getShardIterator(client);

        while (true) {
            // A request to read data from Kinesis stream
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            // Maximum number of records to return
            getRecordsRequest.setLimit(20);

            // Get records from Kinesis
            GetRecordsResult result = client.getRecords(getRecordsRequest);

            // Get list of records from the reply. The result can be empty.
            List<Record> records = result.getRecords();

            for (Record record : records) {
                // Deserialize every record
                Metric metric = fromBytes(record.getData());
                // Display metrics read from Kinesis
                System.out.println(metric);
            }

            // Sleep to avoid
            sleep(200);

            // Read result contains an iterator to read more records
            shardIterator = result.getNextShardIterator();
        }
    }

    private static String getShardIterator(AmazonKinesis client) {
        // Create a request to get an iterator
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        // Specify a name of the stream to read records from
        getShardIteratorRequest.setStreamName(STREAM_NAME);
        // Specify what shard to read from
        getShardIteratorRequest.setShardId(SHARD_ID);
        // Start reading from the oldest record
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

        // Get iterator to read data from a stream from a specific shard
        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
        // Iterator that we can use to read records from the specified shard
        return getShardIteratorResult.getShardIterator();
    }
}
