package com.stage.adapter.mvb.infrastructure;

import java.net.MalformedURLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.RawDataMeasured;

public class RawDataInfrastructure {

	private final String TOPIC = "Meetnet.meting.raw";
	private KafkaProducer<String, RawDataMeasured> producer;
	
	public RawDataInfrastructure(Properties props) {
		producer = new KafkaProducer<String, RawDataMeasured>(props);
	}
	
	//Testpurpose
	public RawDataInfrastructure(
			KafkaProducer<String, RawDataMeasured> producer,
			Properties props
	) {
		this.producer = producer;
	}
	
	public void produce(RawDataMeasured rawDataMeasured) {
		
		final ProducerRecord<String, RawDataMeasured> record = new ProducerRecord<String, RawDataMeasured>(TOPIC, rawDataMeasured.getSensorID(), rawDataMeasured);

		producer.send(record);
		producer.flush();
		System.out.println("ℹ️ Event published");
}
	
}
