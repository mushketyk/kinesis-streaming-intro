package io.codementor.streaming;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KinesisUtils {

    public static final String STREAM_NAME = "metrics-stream";

    public static final String PROCESSOR_NAME = "metrics-processor";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static AmazonKinesis createKinesisClient() {
        return AmazonKinesisClientBuilder
                .standard()
                .build();
    }

    public static AmazonDynamoDB createDynamoDBClient() {
        return AmazonDynamoDBClientBuilder
                .standard()
                .build();
    }

    public static ByteBuffer toBytes(Metric metric) {
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(metric));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Metric fromBytes(ByteBuffer data) {
        try {
            return mapper.readValue(data.array(), Metric.class);
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
