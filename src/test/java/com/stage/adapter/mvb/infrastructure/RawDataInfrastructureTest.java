package com.stage.adapter.mvb.infrastructure;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.RawDataMeasured;

@ExtendWith(MockitoExtension.class)
public class RawDataInfrastructureTest {

	private RawDataInfrastructure rawDataInfrastructure;
	private String TOPIC = "Meetnet.meting.raw";
	private KafkaProducer<String, RawDataMeasured> mockProducer = Mockito.mock(KafkaProducer.class);
	private Properties mockProps;
	
	@BeforeEach
	public void setup() {
		rawDataInfrastructure = new RawDataInfrastructure(
		mockProducer,
		mockProps
		);
	}
	
	@Test
	public void produce() {
		RawDataMeasured data = new RawDataMeasured();
		data.setSensorID("sensorID");
		data.setLocatie("Nieuwpoort");
		data.setEenheid("10.00");
		data.setWaarde("waarde");
		data.setTijdstip(1L);
		
		final ProducerRecord<String, RawDataMeasured> record = new ProducerRecord<String, RawDataMeasured>(TOPIC, data.getSensorID(), data);
		
		rawDataInfrastructure.produce(data);
		
		Mockito.verify(mockProducer, Mockito.times(1)).send(record);
		Mockito.verify(mockProducer, Mockito.times(1)).flush();
		
	}
	
}
