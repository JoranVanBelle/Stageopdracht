package com.stage.adapter.mvb.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.adapter.mvb.repository.WeatherRepository;

@ExtendWith(MockitoExtension.class)
public class WeatherServiceTest {

	private WeatherService weatherService;
	private WeatherRepository weatherRepository = Mockito.mock(WeatherRepository.class);
	
	@BeforeEach
	public void setup() {
		weatherService = new WeatherService(weatherRepository);
	}
	
	@Test
	public void insertDataIntoDatabase_Kiteable() {
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
		  
		  Mockito.when(weatherRepository.insertDataIntoDatabase(weather))
		  	.thenReturn(1);
		  
		  Assertions.assertDoesNotThrow(() -> weatherService.insertDataIntoDatabase(weather));
	}
	
	@Test
	public void insertDataIntoDatabase_Unkiteable() {
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
		  
		  Mockito.when(weatherRepository.insertDataIntoDatabase(weather))
		  	.thenReturn(1);
		  
		  Assertions.assertDoesNotThrow(() -> weatherService.insertDataIntoDatabase(weather));
	}
	
}
