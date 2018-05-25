package io.codementor.streaming;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class KinesisUtils {

    public static final String STREAM_NAME = "metrics-stream";

    public static AmazonKinesis createKinesisClient() {
        return AmazonKinesisClientBuilder
                .standard()
                .build();
    }
}
