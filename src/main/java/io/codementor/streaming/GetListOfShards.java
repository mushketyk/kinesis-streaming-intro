package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

public class GetListOfShards {
    public static void main(String[] args) {
        AmazonKinesis client = KinesisUtils.createKinesisClient();

        String nextToken = null;

        do {
            ListShardsRequest listShardsRequest = new ListShardsRequest();
            if (nextToken == null)
                listShardsRequest.setStreamName(KinesisUtils.STREAM_NAME);
            listShardsRequest.setNextToken(nextToken);
            listShardsRequest.setMaxResults(1);

            System.out.println("Fetching more shards");
            ListShardsResult result = client.listShards(listShardsRequest);

            displayShards(result);

            nextToken = result.getNextToken();
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
