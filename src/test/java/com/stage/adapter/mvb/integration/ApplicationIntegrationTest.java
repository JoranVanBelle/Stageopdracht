package com.stage.adapter.mvb.integration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.adapter.mvb.Application;
import com.stage.adapter.mvb.extension.KafkaTestcontainer;
import com.stage.adapter.mvb.extension.MailhogTestcontainer;

@ExtendWith(MockitoExtension.class)
@ExtendWith(KafkaTestcontainer.class)
@ExtendWith(MailhogTestcontainer.class)
public class ApplicationIntegrationTest {
		
	@Test
	public void applicationIntegrationTest_correctAPI() {
		
		Assertions.assertDoesNotThrow(() -> {
			Application.startApp();
			Thread.sleep(10000);
		});
		
		System.err.println("here");
		System.err.println(System.getenv("BOOTSTRAP_SERVERS"));
	}
	
}
