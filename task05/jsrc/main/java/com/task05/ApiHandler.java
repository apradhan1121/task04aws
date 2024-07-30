package com.task05;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.AmazonServiceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = false,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)

@DependsOn(name="Events", resourceType = ResourceType.DYNAMODB_TABLE)

@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "eu-central-1"),
		@EnvironmentVariable(key = "table", value = "Events"),
} )
public class ApiHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	private static final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		Map<String, String> content;

		try {
			content = objectMapper.readValue(request.getBody(), new TypeReference<Map<String, String>>() {});
		} catch (IOException e) {
			response.setStatusCode(400);
			response.setBody("Error parsing request body: " + e.getMessage());
			return response;
		}

		String id = UUID.randomUUID().toString();
		String createdAt = java.time.Instant.now().toString();

		Map<String, AttributeValue> item = new HashMap<>();
		item.put("id", new AttributeValue(id));
		item.put("principalId", new AttributeValue(content.get("principalId")));
		item.put("createdAt", new AttributeValue(createdAt));

		// Ensure 'body' is stored correctly as a map in DynamoDB
		Map<String, AttributeValue> bodyMap = new HashMap<>();
		content.forEach((key, value) -> bodyMap.put(key, new AttributeValue().withS(value)));
		item.put("body", new AttributeValue().withM(bodyMap));

		try {
			PutItemRequest putItemRequest = new PutItemRequest()
					.withTableName("Events")
					.withItem(item);
			PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		} catch (AmazonServiceException e) {
			response.setStatusCode(500);
			response.setBody("Failed to put item in DynamoDB: " + e.getErrorMessage());
			return response;
		}

		response.setStatusCode(201);
		response.setBody("Event created successfully with ID: " + id);
		return response;
	}
}
