package com.task06;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.OperationType;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.joda.time.Instant;

@LambdaHandler(lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = false,
//		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)

@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)

@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}"),
})

public class AuditProducer implements RequestHandler<DynamodbEvent, String> {
	private static final String AUDIT_TABLE_NAME = System.getenv("table");
	private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());

	@Override
	public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {
		Table auditTable = dynamoDB.getTable(AUDIT_TABLE_NAME);
		System.out.println("dynamodbEvent: " + dynamodbEvent.toString());

		for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {
			if (record == null || record.getEventName() == null) {
				continue;
			}

			System.out.println("record: " + record.toString());
			var streamRecord = record.getDynamodb();
			var keys = streamRecord.getKeys();

			if (keys == null || !keys.containsKey("Category") || !keys.containsKey("key")) { // Changed 'Id' to 'key'
				System.err.println("Keys 'Category' and/or 'key' are missing in the record.");
				continue;
			}

			String category = keys.get("Category").getS();
			String key = keys.get("key").getS(); // Changed 'Id' to 'key'
			Instant modificationTime = Instant.now();
			System.out.println("Category: " + category + ", key: " + key + ", modificationTime: " + modificationTime.toString());

			if (record.getEventName().equals("INSERT")) {
				if (streamRecord.getNewImage() != null) {
					String newCategory = streamRecord.getNewImage().get("Category").getS();
					String newKey = streamRecord.getNewImage().get("key").getS(); // Changed 'Id' to 'key'

					System.out.println("New category: " + newCategory + ", new key: " + newKey);

					Item newItem = new Item()
							.withPrimaryKey("key", UUID.randomUUID().toString(), "Category", category) // Updated primary key names
							.withString("itemKey", category)
							.withString("modificationTime", modificationTime.toString())
							.withMap("newValue", Map.of("key", category, "value", newKey)); // Changed 'Id' to 'key'


					System.out.println("Trying to put new item in Audit table: " + newItem.toString());

					auditTable.putItem(newItem);
				} else {
					System.err.println("NewImage is null.");
				}
			} else if (record.getEventName().equals("MODIFY")) {
				if (streamRecord.getOldImage() != null && streamRecord.getNewImage() != null) {
					String oldValue = streamRecord.getOldImage().get("value").getS();
					String newValue = streamRecord.getNewImage().get("value").getS();
					System.out.println("Old value: " + oldValue + ", new value: " + newValue);

					Item modifiedItem = new Item()
							.withPrimaryKey("key", UUID.randomUUID().toString(), "Category", category) // Updated primary key names
							.withString("itemKey", category)
							.withString("modificationTime", modificationTime.toString())
							.withString("updatedAttribute", "value")
							.withString("oldValue", oldValue) // Changed to String
							.withString("newValue", newValue); // Changed to String


					System.out.println("Trying to put modified item in Audit table: " + modifiedItem.toString());

					auditTable.putItem(modifiedItem);
				} else {
					System.err.println("OldImage or NewImage is null.");
				}
			}
		}

		return "Processed " + dynamodbEvent.getRecords().size() + " records.";
	}
}
