package com.stage.adapter.mvb.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTestcontainer implements BeforeAllCallback, AfterAllCallback {

	private KafkaContainer kafka;
	
	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
		
		kafka.start();
		
	}

	@Override
	public void afterAll(ExtensionContext context) throws Exception {
		kafka.close();
	}
	


}
