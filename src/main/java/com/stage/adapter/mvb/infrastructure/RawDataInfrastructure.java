package com.stage.adapter.mvb.infrastructure;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.RawDataMeasured;

public class RawDataInfrastructure {

	private final String TOPIC;
	private KafkaProducer<String, RawDataMeasured> producer;
	
	public RawDataInfrastructure(Properties props) {
		TOPIC = "Meetnet.meting.raw";
		producer = new KafkaProducer<String, RawDataMeasured>(props);
	}
	
	//Testpurpose
	public RawDataInfrastructure(
			String TOPIC,
			KafkaProducer<String, RawDataMeasured> producer,
			Properties props
	) {
		this.TOPIC = TOPIC;
		this.producer = producer;
	}
	
	private static final Logger logger = LogManager.getLogger(RawDataInfrastructure.class);
	
	public void produce(RawDataMeasured rawDataMeasured) {
		
		final ProducerRecord<String, RawDataMeasured> record = new ProducerRecord<String, RawDataMeasured>(TOPIC, rawDataMeasured.getSensorID(), rawDataMeasured);
		producer.send(record);
		producer.flush();
		
}
	
}
