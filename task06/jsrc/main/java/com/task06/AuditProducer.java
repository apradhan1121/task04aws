package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = false,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, String> {
	private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private final DynamoDB dynamoDb = new DynamoDB(client);
	private final String DYNAMODB_TABLE_NAME = System.getenv("target_table");
	private final Table table = dynamoDb.getTable(DYNAMODB_TABLE_NAME);
	@Override
	public String handleRequest(DynamodbEvent event, Context context) {
		System.out.println("Received event: "+event);

		for(DynamodbEvent.DynamodbStreamRecord record: event.getRecords()){
			String eventName=record.getEventName();
			if(eventName.equals("INSERT")){
				addDataToAuditTable(record.getDynamodb().getNewImage());
				break;
			}
			else if(eventName.equals("MODIFY")){
				modifyDataToAuditTable(record.getDynamodb().getNewImage(),record.getDynamodb().getOldImage());

			}
			else{
				break;
			}
		}
		return "";
	}

	private void modifyDataToAuditTable(Map<String, AttributeValue> newImage, Map<String, AttributeValue> oldImage) {
		System.out.println("New Image: "+newImage);
		System.out.println("Old Image: "+oldImage);
		String key = newImage.get("key").getS();
		int oldValue = Integer.parseInt(oldImage.get("value").getN());
		int newValue = Integer.parseInt(newImage.get("value").getN());
		System.out.println("Old Value: "+oldValue);
		System.out.println("New Value: "+newValue);

		if(newValue!=oldValue){
			Item updateAuditItem= new Item()
					.withPrimaryKey("id",UUID.randomUUID().toString())
					.withString("itemKey",key)
					.withString("modificationTime",DateTimeFormatter.ISO_INSTANT
							.format(Instant.now().atOffset(ZoneOffset.UTC)))
					.withString("updatedAttribute","value")
					.withInt("oldValue",oldValue)
					.withInt("newValue",newValue);

			System.out.println("Update Audit Item: "+updateAuditItem);
			table.putItem(updateAuditItem);
		}
	}

	private void addDataToAuditTable(Map<String, AttributeValue> newImage) {
		System.out.println("New Image: "+newImage);

		String key=newImage.get("key").getS();
		int value=Integer.parseInt(newImage.get("value").getN());

		Map<String,Object> newValue=new HashMap<>();
		newValue.put("key",key);
		newValue.put("value", value);

		System.out.println("New Value: "+newValue);
		Item auditItem=new Item()
				.withPrimaryKey("id",UUID.randomUUID().toString())
				.withString("itemKey",key)
				.withString("modificationTime",DateTimeFormatter.ISO_INSTANT
						.format(Instant.now().atOffset(ZoneOffset.UTC)))
				.withMap("newValue",newValue);

		System.out.println("Audit Item: "+auditItem);
		table.putItem(auditItem);
	}
}