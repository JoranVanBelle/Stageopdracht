package com.stage.adapter.mvb.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.stage.adapter.mvb.entity.Emailaddress;

public class EmailRepository {
	
	private final NamedParameterJdbcTemplate jdbcTemplate;
	
	public EmailRepository(NamedParameterJdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	public List<Emailaddress> collectEmailAddresses(String location) {
		
		List<Emailaddress> emailaddresses = new ArrayList<>();
		
		MapSqlParameterSource paramSource = new MapSqlParameterSource();
		paramSource.addValue("location", location);
		List<Map<String, Object>> emailaddressList = jdbcTemplate.queryForList("SELECT * FROM :location;", paramSource);
		
		for(Map<String, Object> email : emailaddressList) {
			emailaddresses.add(createEmailObject(email));
		}
		
	    return emailaddresses;
		
	}
	
	private Emailaddress createEmailObject(Map<String, Object> email) {
		return new Emailaddress(email.get("email").toString());
	}
	
}
