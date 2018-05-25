package io.codementor.streaming;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.codementor.streaming.KinesisUtils.PROCESSOR_NAME;
import static io.codementor.streaming.KinesisUtils.STREAM_NAME;

public class DeleteAllResources {
    public static void main(String[] args) {
        AmazonKinesis kinesis = KinesisUtils.createKinesisClient();
        deleteStream(kinesis);

        AmazonDynamoDB dynamoDB = KinesisUtils.createDynamoDBClient();
        deleteTable(dynamoDB);
    }

    private static void deleteStream(AmazonKinesis kinesis) {
        try {
            System.out.println("Sending a request to delete stream: " + STREAM_NAME);
            kinesis.deleteStream(STREAM_NAME);
        } catch (ResourceNotFoundException e) {
            System.out.println("Stream not found");
        }
    }

    private static void deleteTable(AmazonDynamoDB dynamoDB) {
        try {
            System.out.println("Sending a request to delete DynamoDB table: " + PROCESSOR_NAME);
            dynamoDB.deleteTable(PROCESSOR_NAME);
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException e) {
            System.out.println("Table not found");
        }
    }
}
