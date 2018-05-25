package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import java.util.concurrent.TimeUnit;

import static io.codementor.streaming.Utils.STREAM_NAME;
import static io.codementor.streaming.Utils.sleep;

/**
 * Creates a Kinesis stream that is used by all other examples.
 */
public class CreateStream {

    public static void main(String[] args) {
        // Create a Kinesis client
        AmazonKinesis client = Utils.createKinesisClient();

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        // Set the name of a new stream
        createStreamRequest.setStreamName(STREAM_NAME);
        // Set initial number of shards
        createStreamRequest.setShardCount(2);
        // Send a request to create a stream
        client.createStream(createStreamRequest);
        // Wait for a stream to be ready to accept read/write requests
        waitForStream(client);
    }

    private static void waitForStream(AmazonKinesis client) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(STREAM_NAME);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            sleep(1000);
            try {
                // Get information about the stream
                DescribeStreamResult describeStreamResponse = client.describeStream(describeStreamRequest);
                // Get description of a stream
                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();

                System.out.println(String.format("The stream is in status %s", streamStatus));
                if (streamStatus.equals( "ACTIVE")) {
                    // Stream is created and operational
                    break;
                }
            }
            catch (ResourceNotFoundException e) {
                // Ignore if stream is not yet available
                System.out.println("Stream was not found");
            }
        }
        if (System.currentTimeMillis() >= endTime) {
            throw new RuntimeException(String.format("Stream %s was never activated", STREAM_NAME));
        }
    }
}
