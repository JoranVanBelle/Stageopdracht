package com.stage.adapter.mvb.consumers;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;

import com.stage.KiteableWeatherDetected;
import com.stage.adapter.mvb.database.Database;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWeatherConsumer extends Thread {

	private final Properties props;
	private static Database database;
	private static JavaMailSender emailSender;
	private Consumer<String, GenericRecord> consumer;
	
	private static final String TOPIC = "Meetnet.meting.kiteable";
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private static final Logger logger = LogManager.getLogger(KiteableWeatherConsumer.class);
	
	public KiteableWeatherConsumer(Properties props) {
		this.props = props;
		this.consumer = new KafkaConsumer<>(props);
		this.database = new Database();
	}
	
	public KiteableWeatherConsumer(MockConsumer<String, GenericRecord> consumer) {
		this.props = null;
		this.consumer = consumer;
	}

	@Override
	public void run() {
		Connection connection;
		try {
			connection = database.getSource().getConnection("user", "admin");
			writeToDatabase(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public void writeToDatabase(Connection connection) {	
		consumer.subscribe(Arrays.asList(TOPIC));
		
		logger.info("ℹ️ Consumer will start writing to the database");
		
		while(!closed.get()) {
			ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, GenericRecord> record : records) {
				GenericRecord value = record.value();
				if(schemaIsKnown(value)) {

					insertDataIntoDatabase(value, connection);
					
				} else {
					throw new IllegalArgumentException("Unknown schema in topic");
				}
			}
		}
	}
	
	private static boolean schemaIsKnown(GenericRecord value) {
		return value.getSchema().getName().equals("KiteableWeatherDetected") || value.getSchema().getName().equals("NoKiteableWeatherDetected");
	}
	
	private static void insertDataIntoDatabase(GenericRecord value, Connection connection) {
		String dataID = value.get("DataID").toString();
		String loc = value.get("Locatie").toString();
		String windspeed = value.get("Windsnelheid").toString();
		String windspeedUnit = value.get("EenheidWindsnelheid").toString();
		String waveheight = value.get("Golfhoogte").toString();
		String waveheightUnit = value.get("EenheidGolfhoogte").toString();
		String winddirection = value.get("Windrichting").toString();
		String winddirectionUnit = value.get("EenheidWindrichting").toString();
		long timestamp = (long) value.get("Tijdstip");
		
		PreparedStatement st;
		try {
			st = connection.prepareStatement("INSERT INTO Kiten (DataID, Loc, Windspeed, WindspeedUnit, Waveheight, WaveheightUnit, Winddirection, WinddirectionUnit, TimestampMeasurment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
			st.setString(1, dataID);
			st.setString(2, loc);
			st.setString(3, windspeed);
			st.setString(4, windspeedUnit);
			st.setString(5, waveheight);
			st.setString(6, waveheightUnit);
			st.setString(7, winddirection);
			st.setString(8, winddirectionUnit);
			st.setLong(9, timestamp);
			st.executeUpdate();
			st.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		};
	}
	
    public void stopConsumer() {
    	closed.set(true);
        consumer.close();
    }
	
	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde() {
		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, System.getenv(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWeatherDetected.configure(serdeConfig, false);
		return kiteableWeatherDetected;
	}
    
}
