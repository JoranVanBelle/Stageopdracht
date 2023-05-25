package com.stage.adapter.mvb.infrastructure;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.stage.RawDataMeasured;

public class RawDataInfrastructure {

	private final String TOPIC = "Meetnet.meting.raw";
	private KafkaProducer<String, RawDataMeasured> producer;
	
	public RawDataInfrastructure(Properties props) {
		producer = new KafkaProducer<String, RawDataMeasured>(props);
	}
	
	//Testpurpose
	public RawDataInfrastructure(
			KafkaProducer<String, RawDataMeasured> producer
	) {
		this.producer = producer;
	}
	
	public void produce(RawDataMeasured rawDataMeasured) {
		
		final ProducerRecord<String, RawDataMeasured> record = new ProducerRecord<String, RawDataMeasured>(TOPIC, rawDataMeasured.getSensorID(), rawDataMeasured);

		producer.send(record);
		producer.flush();
}
	
}
