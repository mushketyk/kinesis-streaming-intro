package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import java.util.concurrent.TimeUnit;

import static io.codementor.streaming.KinesisUtils.STREAM_NAME;

public class CreateStream {

    public static void main(String[] args) {
        AmazonKinesis client = KinesisUtils.createKinesisClient();

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        // Set the name of a new stream
        createStreamRequest.setStreamName(STREAM_NAME);
        // Set initial number of shards
        createStreamRequest.setShardCount(4);

        client.createStream(createStreamRequest);

        waitForStream(client);
    }

    private static void waitForStream(AmazonKinesis client) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(STREAM_NAME);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while ( System.currentTimeMillis() < endTime ) {
            try {
                Thread.sleep( 1000 );
            }
            catch ( Exception e ) {}
            try {
                DescribeStreamResult describeStreamResponse = client.describeStream(describeStreamRequest);
                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                if (streamStatus.equals( "ACTIVE") ) {
                    System.out.println("Stream is ACTIVE");
                    break;
                }
                System.out.println(String.format("Stream in status %s", streamStatus));
            }
            catch ( ResourceNotFoundException e ) {
                System.out.println("Stream was not found");
            }
        }
        if (System.currentTimeMillis() >= endTime) {
            throw new RuntimeException(String.format("Stream %s was never activated", STREAM_NAME));
        }
    }
}
