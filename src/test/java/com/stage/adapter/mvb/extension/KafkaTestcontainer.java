package com.stage.adapter.mvb.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
public class KafkaTestcontainer implements BeforeAllCallback, AfterAllCallback {
	
	@SystemStub
	private EnvironmentVariables environmentVariables;
	
	private KafkaContainer kafka;
	
	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
		
		kafka.start();

		Thread.sleep(1000);
		
		environmentVariables =
				  new EnvironmentVariables()
				    .set("BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
	}

	@Override
	public void afterAll(ExtensionContext context) throws Exception {
		kafka.close();
	}

}
