package com.stage.adapter.mvb.infrastructure;

import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;

import com.stage.KiteableWeatherDetected;

import jakarta.mail.internet.MimeMessage;

@ExtendWith(MockitoExtension.class)
public class EmailInfrastructureTest {

	private EmailInfrastructure mockEmailInfrastructure;
	
	private JavaMailSender mockEmailSender = Mockito.mock(JavaMailSender.class);
	
	@Captor
	private ArgumentCaptor<MimeMessagePreparator> mimeMessagePreparatorCaptor;
	
	@BeforeEach
	public void setup() {
		mockEmailInfrastructure = new EmailInfrastructure(
				mockEmailSender
		);
	}
	
	@Test
	public void sendEmail() throws Exception {
    	KiteableWeatherDetected weather = new KiteableWeatherDetected();
        weather.setDataID("NieuwpoortKiteable1");
        weather.setLocatie("Nieuwpoort");
        weather.setWindsnelheid("10.00");
        weather.setEenheidWindsnelheid("m/s");
        weather.setGolfhoogte("151.00");
        weather.setEenheidGolfhoogte("cm");
        weather.setWindrichting("10.00");
        weather.setEenheidWindrichting("deg");
        
        List<String> emails = new ArrayList<>();
        emails.add("test@email.com");
        emails.add("test2@email.com");
        
        mockEmailInfrastructure.sendEmail(weather, emails);
        
        Mockito.verify(mockEmailSender).send(mimeMessagePreparatorCaptor.capture());
        MimeMessagePreparator mimeMessagePreparator = mimeMessagePreparatorCaptor.getValue();
        MimeMessage mimeMessage = Mockito.mock(MimeMessage.class);
        mimeMessagePreparator.prepare(mimeMessage);
        MimeMessageHelper mimeMessageHelper = new MimeMessageHelper(mimeMessage, true);
        mimeMessageHelper.setValidateAddresses(false);
        mimeMessageHelper.setTo(emails.toArray(new String[0]));
        mimeMessageHelper.setSubject("Kiteable weather detected in Test Location");
        
        String expectedText = String.format(
        		"Kiteable weather detected at: %s%n"
        		+ "Location: %s%n"
        		+ "Windspeed: %s %s%n"
        		+ "Waveheight: %s %s%n"
        		+ "Winddirection: %s %s%n", 
        		weather.getLocatie(), 
        		weather.getWindsnelheid(), 
        		weather.getEenheidWindsnelheid(), 
        		weather.getGolfhoogte(), 
        		weather.getEenheidGolfhoogte(), 
        		weather.getWindrichting(), 
        		weather.getEenheidWindrichting(), 
        		weather.getTijdstip());
        
        mimeMessageHelper.setText(expectedText, true);
        verify(mockEmailSender).send(mimeMessagePreparator);
        
	}
	
}
