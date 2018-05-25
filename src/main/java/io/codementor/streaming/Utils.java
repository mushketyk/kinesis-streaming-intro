package io.codementor.streaming;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Utils {

    public static final String STREAM_NAME = "metrics-stream";

    // A name of KCL processor.
    public static final String KCL_PROCESSOR_NAME = "metrics-processor";
    public static final String KCL_DYNAMODB_TABLE = KCL_PROCESSOR_NAME;

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

    /**
     * Serialize a metric object into JSON.
     *
     * @param metric metric object to serialize
     * @return a serialized metric object
     */
    public static ByteBuffer toBytes(Metric metric) {
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(metric));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize a metric object from a JSON object.
     * @param data JSON object to deserialize
     * @return deserialized metrics object
     */
    public static Metric fromBytes(ByteBuffer data) {
        try {
            return mapper.readValue(data.array(), Metric.class);
        } catch (IOException e) {
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
