package com.stage.adapter.mvb.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.stage.KiteableWeatherDetected;
import com.stage.adapter.mvb.entity.Emailaddress;
import com.stage.adapter.mvb.infrastructure.EmailInfrastructure;
import com.stage.adapter.mvb.repository.EmailRepository;

public class EmailService {

	private final EmailInfrastructure emailInfrastructure;
	private final EmailRepository emailRepository;
	
	public EmailService(NamedParameterJdbcTemplate jdbcTemplate) {
		this.emailInfrastructure = new EmailInfrastructure();
		this.emailRepository = new EmailRepository(jdbcTemplate);
	}
	
	//Testpurposes
	public EmailService(
			EmailInfrastructure emailInfrastructure,
			EmailRepository emailRepository
	) {
		this.emailInfrastructure = emailInfrastructure;
		this.emailRepository = emailRepository;
	}
	
	public void sendEmail(KiteableWeatherDetected kiteable) {
		
		List<String> emailaddresses = collectEmailAddresses(kiteable.getLocatie());
		
		emailInfrastructure.sendEmail(kiteable, emailaddresses);
	}
	
	public List<String> collectEmailAddresses(String location) {
		List<Emailaddress> emailaddresses = emailRepository.collectEmailAddresses(location);
		List<String> emailaddressesString = new ArrayList<>();
		
		for(Emailaddress email : emailaddresses) {
			emailaddressesString.add(email.getEmailaddress());
		}
		
		return emailaddressesString;
	}
	
}