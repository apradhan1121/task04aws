package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}"),
})

public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {

		context.getLogger().log("Received request: " + request.toString());
		Map<String, Object> body = (Map<String, Object>) request.get("body");
		if (body == null) {
			body = request;
		}
		context.getLogger().log("Parsed body: " + body.toString());
		Map<String, AttributeValue> itemValues = new HashMap<>();
		String uuid = UUID.randomUUID().toString();
		itemValues.put("id", new AttributeValue().withS(uuid));
		Integer principalId = (Integer) body.getOrDefault("principalId", 0);
		context.getLogger().log("Principal ID: " + principalId);
		itemValues.put("principalId", new AttributeValue().withN(String.valueOf(principalId)));
		Map<String, String> content = (Map<String, String>) body.getOrDefault("content", new HashMap<>());
		context.getLogger().log("Content: " + content.toString());
		Map<String, AttributeValue> contentAttributes = new HashMap<>();
		content.forEach((key, value) -> contentAttributes.put(key, new AttributeValue().withS(value)));
		itemValues.put("body", new AttributeValue().withM(contentAttributes));

		String createdAt = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
		context.getLogger().log("Created At: " + createdAt);
		itemValues.put("createdAt", new AttributeValue().withS(createdAt));

		context.getLogger().log("Item Values to put in DynamoDB: " + itemValues.toString());

		try {
			AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
					.withRegion(System.getenv("region"))
					.build();
			client.putItem(System.getenv("table"), itemValues);
		} catch (Exception e) {
			context.getLogger().log("Error putting item in DynamoDB: " + e.getMessage());
			Map<String, Object> errorResponse = new HashMap<>();
			errorResponse.put("statusCode", 500);
			errorResponse.put("error", "Failed to put item in DynamoDB: " + e.getMessage());
			return errorResponse;
		}

		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", 201);

		Map<String, Object> event = new HashMap<>();
		event.put("id", uuid);
		event.put("principalId", principalId);
		event.put("createdAt", createdAt);
		Map<String, String> simpleBody = new HashMap<>();
		contentAttributes.forEach((key, value) -> simpleBody.put(key, value.getS()));
		context.getLogger().log("Body for response: " + simpleBody.toString());
		event.put("body", simpleBody);

		response.put("event", event);
		context.getLogger().log("Response: " + response.toString());

		return response;
	}
}