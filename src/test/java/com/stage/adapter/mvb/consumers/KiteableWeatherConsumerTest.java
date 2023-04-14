package com.stage.adapter.mvb.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.database.Database;
import com.stage.adapter.mvb.service.EmailService;
import com.stage.adapter.mvb.service.WeatherService;

@ExtendWith(MockitoExtension.class)
public class KiteableWeatherConsumerTest {
	
	private KiteableWeatherConsumer kiteableWeatherConsumer;
	
	private Consumer<String, GenericRecord> mockConsumer = Mockito.mock(Consumer.class);
	private WeatherService mockWeatherService = Mockito.mock(WeatherService.class);
	private EmailService mockEmailService = Mockito.mock(EmailService.class);
	private NamedParameterJdbcTemplate mockJdbcTemplate = Mockito.mock(NamedParameterJdbcTemplate.class);
	private Database mockDatabase = Mockito.mock(Database.class);
	
	private static final String TOPIC = "Meetnet.meting.kiteable";
	
	@BeforeEach
	public void setup() {
		kiteableWeatherConsumer = new KiteableWeatherConsumer(
				mockConsumer,
				mockWeatherService,
				mockEmailService,
				mockJdbcTemplate,
				mockDatabase
		);
	}
	
	@Test
	public void writeToDatabaseTest_OneKiteable() {
		
    	KiteableWeatherDetected weather = new KiteableWeatherDetected();
        weather.setDataID("NieuwpoortKiteable1");
        weather.setLocatie("Nieuwpoort");
        weather.setWindsnelheid("10.00");
        weather.setEenheidWindsnelheid("m/s");
        weather.setGolfhoogte("151.00");
        weather.setEenheidGolfhoogte("cm");
        weather.setWindrichting("10.00");
        weather.setEenheidWindrichting("deg");
        
        ConsumerRecord<String, GenericRecord> record = new ConsumerRecord<>("test", 1, 0L, weather.getDataID(), weather);
        ConsumerRecords<String, GenericRecord> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, 1), Collections.singletonList(record)));        
        
		Mockito.when(mockConsumer.poll(Mockito.any(Duration.class))).thenReturn(records);
		Mockito.doNothing().when(mockEmailService).sendEmail(weather);
		Mockito.doNothing().when(mockWeatherService).insertDataIntoDatabase(weather);
		
		Assertions.assertDoesNotThrow(() -> kiteableWeatherConsumer.consume(mockJdbcTemplate));
		Mockito.verify(mockEmailService, Mockito.times(1)).sendEmail(weather);
		Mockito.verify(mockWeatherService, Mockito.times(1)).insertDataIntoDatabase(weather);
	}
	
	@Test
	public void writeToDatabaseTest_OneNoKiteable() {
		
    	NoKiteableWeatherDetected weather = new NoKiteableWeatherDetected();
        weather.setDataID("Nieuwpoortiteable1");
        weather.setLocatie("Nieuwpoort");
        weather.setWindsnelheid("10.00");
        weather.setEenheidWindsnelheid("m/s");
        weather.setGolfhoogte("151.00");
        weather.setEenheidGolfhoogte("cm");
        weather.setWindrichting("10.00");
        weather.setEenheidWindrichting("deg");
        
        ConsumerRecord<String, GenericRecord> record = new ConsumerRecord<>("test", 1, 0L, weather.getDataID(), weather);
        ConsumerRecords<String, GenericRecord> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, 1), Collections.singletonList(record)));        
        
		Mockito.when(mockConsumer.poll(Mockito.any(Duration.class))).thenReturn(records);
		Mockito.doNothing().when(mockEmailService).sendEmail(null);
		Mockito.doNothing().when(mockWeatherService).insertDataIntoDatabase(weather);
		
		Assertions.assertDoesNotThrow(() -> kiteableWeatherConsumer.consume(mockJdbcTemplate));
		Mockito.verify(mockEmailService, Mockito.times(0)).sendEmail(null);
		Mockito.verify(mockWeatherService, Mockito.times(1)).insertDataIntoDatabase(weather);
	}
	
	@Test
	public void writeToDatabaseTest_wrongSchema() {
		
    	RawDataMeasured weather = new RawDataMeasured();
        weather.setSensorID("sensorID");
        weather.setLocatie("Nieuwpoort");
        weather.setWaarde("10.00");
        weather.setEenheid("deg");
        weather.setTijdstip(1L);
        
        ConsumerRecord<String, GenericRecord> record = new ConsumerRecord<>("test", 1, 0L, weather.getSensorID(), weather);
        ConsumerRecords<String, GenericRecord> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(TOPIC, 1), Collections.singletonList(record)));        
        
		Mockito.when(mockConsumer.poll(Mockito.any(Duration.class))).thenReturn(records);
		Mockito.doNothing().when(mockEmailService).sendEmail(null);
		Mockito.doNothing().when(mockWeatherService).insertDataIntoDatabase(weather);
		
		Assertions.assertThrows(IllegalArgumentException.class, () -> kiteableWeatherConsumer.consume(mockJdbcTemplate));
	}

}
