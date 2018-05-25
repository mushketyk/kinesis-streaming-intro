package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;

import static io.codementor.streaming.KinesisUtils.STREAM_NAME;

public class DeleteStream {
    public static void main(String[] args) {
        AmazonKinesis client = KinesisUtils.createKinesisClient();

        client.deleteStream(STREAM_NAME);
        System.out.println("Sent a request to delete stream: " + STREAM_NAME);
    }
}
