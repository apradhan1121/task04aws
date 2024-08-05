package com.task07;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "uuid_generator",
		roleName = "uuid_generator-role",
		isPublishVersion = false,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@RuleEventSource(
		targetRule = "uuid_trigger"
)
@DependsOn(
		name = "uuid_trigger",
		resourceType = ResourceType.CLOUDWATCH_RULE
)
@EnvironmentVariables(value = {
			@EnvironmentVariable(key = "target_bucket", value = "${target_bucket}"),
			@EnvironmentVariable(key = "region",value = "${region}")
})
public class UuidGenerator implements RequestHandler<ScheduledEvent, Map<String, Object>> {

	private static final String BUCKET_NAME = System.getenv("target_bucket");

	@Override
	public Map<String, Object> handleRequest(ScheduledEvent event, Context context) {
		AmazonS3 amazonS3 = AmazonS3Client.builder().withRegion(System.getenv("region")).build();
		String key = Instant.now().toString();

		List<String> uuids = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			uuids.add(UUID.randomUUID().toString());
		}

		String content = "{\n  \"ids\": [\n    \"" + String.join("\",\n    \"", uuids) + "\"\n  ]\n}";
		File file = new File("/tmp/AWS.txt");

		try (FileWriter writer = new FileWriter(file)) {
			writer.write(content);
		} catch (IOException e) {
			e.printStackTrace();
			Map<String, Object> errorResult = new HashMap<>();
			errorResult.put("statusCode", 500);
			errorResult.put("body", "Error writing to file: " + e.getMessage());
			return errorResult;
		}

		amazonS3.putObject(BUCKET_NAME, key, file);

		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "UUIDs generated and stored in S3 bucket");

		return resultMap;
	}
}
