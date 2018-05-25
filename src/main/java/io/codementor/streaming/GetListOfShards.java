package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * Get information about shards in the stream.
 */
public class GetListOfShards {
    public static void main(String[] args) {
        AmazonKinesis client = Utils.createKinesisClient();

        // A single request to get a list of shards can return at most
        // 1,000 shards. If you have more than 1,000 shards you
        // need to perform additional requests using the "nextToken"
        // value which is null by default
        String nextToken = null;

        do {
            ListShardsRequest listShardsRequest = new ListShardsRequest();
            // To get a list of shards from a stream we either need
            // to provide a name of a stream (if this is our first request)
            // or next token for subsequent requests
            if (nextToken == null)
                listShardsRequest.setStreamName(Utils.STREAM_NAME);
            else
                listShardsRequest.setNextToken(nextToken);

            System.out.println("Fetching Kinesis shards");
            ListShardsResult result = client.listShards(listShardsRequest);

            displayShards(result);

            // Get next token to get more shards
            nextToken = result.getNextToken();

            // If we have more shards to get, repeat the loop
        } while (nextToken != null);
    }

    private static void displayShards(ListShardsResult result) {
        for (Shard shard : result.getShards()) {
            // Get hash key range a shard is responsible for
            HashKeyRange hashKeyRange = shard.getHashKeyRange();
            // Returns first a range of records' sequence numbers in a stream
            SequenceNumberRange sequenceNumberRange = shard.getSequenceNumberRange();
            // Get a shard id that we can use to read data from it
            String shardId = shard.getShardId();
            // Get parent's shard id
            String parentShardId = shard.getParentShardId();

            System.out.println(String.format("Shard: %s", shardId));
            System.out.println(String.format("Parent shard: %s", parentShardId));
            System.out.println(String.format("Hash key range: %s", hashKeyRange));
            System.out.println(String.format("Starting sequence number: %s", sequenceNumberRange));

            System.out.println();
        }
    }
}
