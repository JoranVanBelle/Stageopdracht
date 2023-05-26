package com.stage.adapter.mvb.service;

import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.infrastructure.RawDataInfrastructure;

@ExtendWith(MockitoExtension.class)
public class RawDataServiceTest {

	private RawDataService rawDataService;
	private RawDataInfrastructure rawDataInfrastructure = Mockito.mock(RawDataInfrastructure.class);
	
	private Properties props = Mockito.mock(Properties.class);
	
	@BeforeEach
	public void setup() {
		rawDataService = new RawDataService(rawDataInfrastructure);
	}
	
	@Test
	public void produceTest() {
		RawDataMeasured data = new RawDataMeasured();
		data.setSensorID("sensorID");
		data.setLocatie("Nieuwpoort");
		data.setEenheid("10.00");
		data.setWaarde("waarde");
		data.setTijdstip(1L);
		
		Mockito.doNothing().when(rawDataInfrastructure).produce(Mockito.any(RawDataMeasured.class));
	
		Assertions.assertDoesNotThrow(() -> rawDataService.produce(data));
	}
	
}
