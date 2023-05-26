package com.stage.adapter.mvb.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.service.RawDataService;

public class RawDataMeasuredProducer {
	
	private String sensorId;
	private String locatie;
	private String waarde;
	private String eenheid;
	private Long tijdstip;
	
	private RawDataService rawDataService;
	
	public RawDataMeasuredProducer(Properties props, String sensorId, String locatie, String waarde, String eenheid, Long tijdstip) {
		this.sensorId = sensorId;
		this.locatie = locatie;
		this.waarde = waarde;
		this.eenheid = eenheid;
		this.tijdstip = tijdstip;
		this.rawDataService = new RawDataService(props);
	}
	
	public void createEvent() {
		
		final RawDataMeasured rdm = new RawDataMeasured();
		rdm.setSensorID(sensorId);
		rdm.setLocatie(locatie);
		rdm.setWaarde(waarde);
		rdm.setEenheid(eenheid);
		rdm.setTijdstip(tijdstip);
		rawDataService.produce(rdm);
	}
}
