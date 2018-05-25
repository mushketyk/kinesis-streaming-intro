package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

import static io.codementor.streaming.KinesisUtils.STREAM_NAME;
import static io.codementor.streaming.KinesisUtils.fromBytes;
import static io.codementor.streaming.KinesisUtils.sleep;

public class SingleShardConsumer {

    public static final String SHARD_ID = "shardId-000000000000";

    public static void main(String[] args) {

        AmazonKinesis client = KinesisUtils.createKinesisClient();
        String shardIterator = getShardIterator(client);

        while (true) {

            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(20);

            GetRecordsResult result = client.getRecords(getRecordsRequest);

            // Put the result into record list. The result can be empty.
            List<Record> records = result.getRecords();

            for (Record record : records) {
                Metric metric = fromBytes(record.getData());
                System.out.println(metric);
            }

            sleep(200);
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
