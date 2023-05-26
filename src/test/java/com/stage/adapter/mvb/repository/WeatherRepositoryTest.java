package com.stage.adapter.mvb.repository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;

@ExtendWith(MockitoExtension.class)
public class WeatherRepositoryTest {

	private WeatherRepository weatherRepository;
	
	private NamedParameterJdbcTemplate jdbcTemplate = Mockito.mock(NamedParameterJdbcTemplate.class);
	
	@BeforeEach
	public void setup() {
		weatherRepository = new WeatherRepository(jdbcTemplate);
	}
	
	@Test
	public void insertDataIntoDatabaseTest_Kiteable() {
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
		  
		  Mockito.when(jdbcTemplate.update(Mockito.anyString(), Mockito.any(SqlParameterSource.class)))
		  .thenReturn(1);
		  
		  int rowsAffected = weatherRepository.insertDataIntoDatabase(weather);
		  
		  Assertions.assertEquals(1, rowsAffected);
	}
	
	@Test
	public void insertDataIntoDatabaseTest_Unkiteable() {
		  NoKiteableWeatherDetected weather = new NoKiteableWeatherDetected();
		  weather.setDataID("DataID");
		  weather.setLocatie("Nieuwpoort");
		  weather.setWindsnelheid("10.00");
		  weather.setEenheidWindsnelheid("m/s");
		  weather.setGolfhoogte("151.00");
		  weather.setEenheidGolfhoogte("cm");
		  weather.setWindrichting("10.00");
		  weather.setEenheidWindrichting("deg");
		  weather.setTijdstip(1L);
		  
		  Mockito.when(jdbcTemplate.update(Mockito.anyString(), Mockito.any(SqlParameterSource.class)))
		  .thenReturn(1);
		  
		  int rowsAffected = weatherRepository.insertDataIntoDatabase(weather);
		  
		  Assertions.assertEquals(1, rowsAffected);
	}
}
