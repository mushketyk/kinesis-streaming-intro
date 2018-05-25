package io.codementor.streaming;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import static io.codementor.streaming.Utils.KCL_DYNAMODB_TABLE;
import static io.codementor.streaming.Utils.STREAM_NAME;

/**
 * Delete all resources created by examples in this project.
 */
public class DeleteAllResources {
    public static void main(String[] args) {
        AmazonKinesis kinesis = Utils.createKinesisClient();
        deleteStream(kinesis);

        AmazonDynamoDB dynamoDB = Utils.createDynamoDBClient();
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
            System.out.println("Sending a request to delete DynamoDB table: " + KCL_DYNAMODB_TABLE);
            dynamoDB.deleteTable(KCL_DYNAMODB_TABLE);
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException e) {
            System.out.println("Table not found");
        }
    }
}
