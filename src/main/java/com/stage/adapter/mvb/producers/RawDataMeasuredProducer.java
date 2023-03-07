package com.stage.adapter.mvb.producers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import com.data.RawDataMeasured;

public class RawDataMeasuredProducer {

	private final Properties props;
	private static final String TOPIC = "Meetnet.meting.raw";
	private String sensorId;
	private String locatie;
	private String waarde;
	private String eenheid;
	private Long tijdstip;
	
	public RawDataMeasuredProducer(Properties props, String sensorId, String locatie, String waarde, String eenheid, Long tijdstip) {
		this.props = props;
		this.sensorId = sensorId;
		this.locatie = locatie;
		this.waarde = waarde;
		this.eenheid = eenheid;
		this.tijdstip = tijdstip;
	}
	
	public void createEvent() {
		
		try(KafkaProducer<String, RawDataMeasured> producer = new KafkaProducer<String, RawDataMeasured>(props)) {
			final RawDataMeasured rdm = new RawDataMeasured();
			rdm.setSensorID(sensorId);
			rdm.setLocatie(locatie);
			rdm.setWaarde(waarde);
			rdm.setEenheid(eenheid);
			rdm.setTijdstip(tijdstip);
			
			final ProducerRecord<String, RawDataMeasured> record = new ProducerRecord<String, RawDataMeasured>(TOPIC, rdm.getSensorID(), rdm);
			producer.send(record);
			producer.flush();
		} catch (SerializationException e) {
            e.printStackTrace();
        }
		
	}
	
	private Schema getSchema() {
		
		final int version = 1;
		final String registry = String.format("http://localhost:8081/subjects/_mvbschemas/versions/%d/schema", version);
		HttpURLConnection con;
		Schema schema;
		
		try {
			con = (HttpURLConnection) new URL(registry).openConnection();
			con.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");
			int status = con.getResponseCode();
			con.setRequestMethod("GET");
			
			if (status == 200) {
			    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			    String inputLine;
			    StringBuilder response = new StringBuilder();
			    while ((inputLine = in.readLine()) != null) {
			        response.append(inputLine);
			    }
			    in.close();
			    String schemaString = response.toString();
			    schema = new Schema.Parser().parse(schemaString);
				return schema;
			} else {
				System.err.println("Schema is not available");
			}
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
			
		} catch (IOException e) {
			e.printStackTrace();
			
		}
		
		return null;
	}
}
