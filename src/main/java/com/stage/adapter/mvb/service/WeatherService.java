package com.stage.adapter.mvb.service;

import org.apache.avro.generic.GenericRecord;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.stage.adapter.mvb.repository.WeatherRepository;

public class WeatherService {
	
	private final WeatherRepository weatherRepository;
	
	public WeatherService(NamedParameterJdbcTemplate jdbcTemplate) {
		this.weatherRepository = new WeatherRepository(jdbcTemplate);
	}
	
	//Testpurposes
	public WeatherService(WeatherRepository weatherRepository) {
		this.weatherRepository = weatherRepository;
	}
	
	public void insertDataIntoDatabase(GenericRecord value) {
		weatherRepository.insertDataIntoDatabase(value);
	}
	
}
