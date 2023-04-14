package com.stage.adapter.mvb.extension;

import java.util.Random;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

import io.restassured.RestAssured;

public class MailhogTestcontainer implements BeforeAllCallback, AfterAllCallback {

	private static final Random random = new Random();
	
    private static final Integer PORT_SMTP = 1025;
    private static final Integer PORT_HTTP = 8025;

    Integer smtpPort;
    String smtpHost;
    
    private GenericContainer<?> mailhog;
	
	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		 mailhog = new GenericContainer<>("mailhog/mailhog")
			        .withExposedPorts(PORT_SMTP, PORT_HTTP)
			        .withCreateContainerCmdModifier( cmd -> cmd.withName( "mailhog-" + (random.nextInt() & Integer.MAX_VALUE)));
		 
		 mailhog.start();

        smtpPort = mailhog.getMappedPort(PORT_SMTP);
        smtpHost = mailhog.getHost();
        Integer httpPort = mailhog.getMappedPort(PORT_HTTP);
        
        RestAssured.baseURI = "http://" + smtpHost;
        RestAssured.port = httpPort;
        RestAssured.basePath = "/api/v1";
	}

	@Override
	public void afterAll(ExtensionContext context) throws Exception {
		mailhog.close();
		
	}

}
