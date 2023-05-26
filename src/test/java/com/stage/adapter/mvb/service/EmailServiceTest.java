package com.stage.adapter.mvb.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.KiteableWeatherDetected;
import com.stage.adapter.mvb.entity.Emailaddress;
import com.stage.adapter.mvb.infrastructure.EmailInfrastructure;
import com.stage.adapter.mvb.repository.EmailRepository;

@ExtendWith(MockitoExtension.class)
public class EmailServiceTest {

	private EmailService emailService;
	private EmailInfrastructure emailInfrastructure = Mockito.mock(EmailInfrastructure.class);
	private EmailRepository emailRepository = Mockito.mock(EmailRepository.class);
	
	@BeforeEach()
	public void setup() {
		emailService = new EmailService(
		emailInfrastructure,
		emailRepository
		);
	}
	
	@Test
	public void sendEmailTest() {
		List<Emailaddress> mockEmails = new ArrayList<>();
		mockEmails.add(new Emailaddress("test@email.com"));
		mockEmails.add(new Emailaddress("test2@email.com"));
		
		KiteableWeatherDetected weather = new KiteableWeatherDetected();
		weather.setDataID("DataID");
		weather.setLocatie("Nieuwpoort");
		weather.setWindsnelheid("10.00");
		weather.setEenheidWindsnelheid("m/s");
		weather.setGolfhoogte("151.00");
		weather.setEenheidGolfhoogte("cm");
		weather.setWindrichting("10.00");
		weather.setEenheidWindrichting("deg");
		weather.setTijdstip(1L);
		
		Mockito.when(emailRepository.collectEmailAddresses(Mockito.anyString()))
			.thenReturn(mockEmails);
		
		Assertions.assertDoesNotThrow(() -> emailService.sendEmail(weather));
	}
	
	@Test
	public void collectEmailAddressesTest() {
		List<Emailaddress> mockEmails = new ArrayList<>();
		mockEmails.add(new Emailaddress("test@email.com"));
		mockEmails.add(new Emailaddress("test2@email.com"));
		
		Mockito.when(emailRepository.collectEmailAddresses(Mockito.anyString()))
			.thenReturn(mockEmails);
		
		List<String> expected = new ArrayList<>();
		expected.add("test2@email.com");
		expected.add("test@email.com");
		
		List<String> result = emailService.collectEmailAddresses("Nieuwpoort");
		
		Assertions.assertEquals(2, result.size());
		Assertions.assertEquals("test@email.com", result.get(0));
		Assertions.assertEquals("test2@email.com", result.get(1));
		
	}
}
