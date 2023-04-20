package com.stage.adapter.mvb.infrastructure;

import java.util.List;
import java.util.Properties;

import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;

import com.stage.KiteableWeatherDetected;

public class EmailInfrastructure {


	private JavaMailSender emailSender;
	
	public EmailInfrastructure(String host, int port) {
		emailSender = getMailingProps(host, port);
	}
	
	// Testpurpose
	public EmailInfrastructure(
			JavaMailSender emailSender
	) {
		this.emailSender = emailSender;
	}
	
	public void sendEmail(KiteableWeatherDetected content, List<String> emails) throws MailException {

	    if(emails != null) {
	    	
	    	String[] array = emails.toArray(new String[emails.size()]);
	    	
		    MimeMessagePreparator messagePreparator = mimeMessage -> {
		        MimeMessageHelper messageHelper = new MimeMessageHelper(mimeMessage, true);
		        messageHelper.setFrom("joran.vanbelle@live.be");
		        messageHelper.setReplyTo("joran.vanbelle@live.be");
		        messageHelper.setTo(array);
		        messageHelper.setSubject(String.format("Kiteable weather detected in %s", content.getLocatie()));
		        messageHelper.setText(getText(content));
		    };

		    emailSender.send(messagePreparator);
	    }
	    
	}
	
	private static JavaMailSender getMailingProps(String host, int port) {
		
		JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
		
		mailSender.setHost(host);
		mailSender.setPort(port);
		
		Properties props = mailSender.getJavaMailProperties();
	    props.put("mail.transport.protocol", "smtp");
	    props.put("mail.smtp.auth", "false");
	    props.put("mail.smtp.starttls.enable", "false");
	    props.put("mail.debug", "true");
		
		return mailSender;
	}
	
	private String getText(KiteableWeatherDetected weather) {
		return String.format(
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
	}
}
