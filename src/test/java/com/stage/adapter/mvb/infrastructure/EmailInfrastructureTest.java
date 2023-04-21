package com.stage.adapter.mvb.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.icegreen.greenmail.junit.GreenMailRule;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMultipart;
import org.junit.Before;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.KiteableWeatherDetected;

import jakarta.mail.internet.MimeMessage;

@ExtendWith(MockitoExtension.class)
public class EmailInfrastructureTest {

	private GreenMail greenMail;
	private EmailInfrastructure emailInfrastructure;

	@BeforeEach
	public void setup() {
		greenMail = new GreenMail(ServerSetupTest.SMTP);
		greenMail.start();
		emailInfrastructure = new EmailInfrastructure("localhost", greenMail.getSmtp().getPort());
	}

	@AfterEach
	public void tearDown() {
		greenMail.stop();
	}

	@Test
	public void givenEmailMessageWithAttachment_whenEmailIsSent_MessageIsReceived() throws Exception {

		KiteableWeatherDetected weather = new KiteableWeatherDetected();
        weather.setDataID("NieuwpoortKiteable1");
        weather.setLocatie("Nieuwpoort");
        weather.setWindsnelheid("10.00");
        weather.setEenheidWindsnelheid("m/s");
        weather.setGolfhoogte("151.00");
        weather.setEenheidGolfhoogte("cm");
        weather.setWindrichting("10.00");
        weather.setEenheidWindrichting("deg");

		emailInfrastructure.sendEmail(weather, Arrays.asList("joran.vanbelle@live.be"));

		MimeMessage[] receivedMessages = greenMail.getReceivedMessages();
		assertEquals(1, receivedMessages.length);

		MimeMessage receivedMessage = receivedMessages[0];
		assertEquals("Kiteable weather detected at Nieuwpoort", subjectFromMessage(receivedMessage));
		assertEquals(EmailInfrastructure.getText(weather), emailTextFrom(receivedMessage));
	}

	private static String subjectFromMessage(MimeMessage receivedMessage) throws MessagingException, MessagingException {
		return receivedMessage.getSubject();
	}

	private static String emailTextFrom(MimeMessage receivedMessage) throws IOException, MessagingException, IOException {
		return ((MimeMultipart) receivedMessage.getContent())
				.getBodyPart(0)
				.getContent()
				.toString();
	}

}
