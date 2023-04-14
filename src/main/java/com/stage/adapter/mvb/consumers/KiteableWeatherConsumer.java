package com.stage.adapter.mvb.consumers;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.mail.MailException;

import com.stage.KiteableWeatherDetected;
import com.stage.adapter.mvb.database.Database;
import com.stage.adapter.mvb.service.EmailService;
import com.stage.adapter.mvb.service.WeatherService;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWeatherConsumer extends Thread {
	
	private Database database;
	private NamedParameterJdbcTemplate jdbcTemplate;
	private Consumer<String, GenericRecord> consumer;
	private final WeatherService weatherService;
	private final EmailService emailService;
	
	private static final String TOPIC = "Meetnet.meting.kiteable";
	
	private static final Logger logger = LogManager.getLogger(KiteableWeatherConsumer.class);
	
	public KiteableWeatherConsumer(Properties props, String database_url, String database_user, String database_password) {
		this.consumer = new KafkaConsumer<>(props);
		this.database = new Database();
		this.jdbcTemplate = new NamedParameterJdbcTemplate(database.getPGPoolingDataSource(database_url, database_user, database_password));
		this.weatherService = new WeatherService(jdbcTemplate);
		this.emailService = new EmailService(jdbcTemplate);
	}
	
	// Test purpose
	public KiteableWeatherConsumer(
			Consumer<String, GenericRecord> consumer,
			WeatherService weatherService,
			EmailService emailService,
			NamedParameterJdbcTemplate jdbcTemplate,
			Database database
	) {
		this.consumer = consumer;
		this.weatherService = weatherService;
		this.emailService = emailService;
		this.jdbcTemplate = jdbcTemplate;
		this.database = database;
	}
	
	@Override
	public void run() {
		try {		
			writeToDatabase(this.jdbcTemplate);
			
		} catch (MailException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
            consumer.close();
        } 
	}
	
	public void writeToDatabase(NamedParameterJdbcTemplate jdbcTemplate) throws MailException, SQLException {	
		consumer.subscribe(Arrays.asList(TOPIC));
		
		logger.info("ℹ️ Consumer will start writing to the database");
		
		while(true) {
			consume(jdbcTemplate);
		}
	}
	
	public void consume(NamedParameterJdbcTemplate jdbcTemplate) {
		ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
		for(ConsumerRecord<String, GenericRecord> record : records) {
			GenericRecord value = record.value();
			if(schemaIsKnown(value)) {
				if(schemaIsKiteable(value)) {
					var kiteable = (KiteableWeatherDetected) SpecificData.get().deepCopy(value.getSchema(), value);
					emailService.sendEmail(kiteable);
				}
				
				weatherService.insertDataIntoDatabase(value);
				
			} else {
				throw new IllegalArgumentException("Unknown schema in topic");
			}
		}
	}
	
	private static boolean schemaIsKnown(GenericRecord value) {
		return value.getSchema().getName().equals("KiteableWeatherDetected") || value.getSchema().getName().equals("NoKiteableWeatherDetected");
	}
	
	private static boolean schemaIsKiteable(GenericRecord value) {
		return value.getSchema().getName().equals("KiteableWeatherDetected");
	}
	
	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde() {
		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, System.getenv(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWeatherDetected.configure(serdeConfig, false);
		return kiteableWeatherDetected;
	}
}
