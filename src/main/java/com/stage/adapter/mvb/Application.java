package com.stage.adapter.mvb;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import com.stage.adapter.mvb.consumers.KiteableWeatherConsumer;
import com.stage.adapter.mvb.helpers.ApplicationHelper;
import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.streams.MergedWeatherStream;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class Application {

//	private static final String[] sensoren = {"A2BGHA", "WDLGHA", "RA2GHA", "OSNGHA", "NPBGHA", "SWIGHA",
//			"MP0WC3", "MP7WC3", "NP7WC3", "MP0WVC", "MP7WVC", "NP7WVC", "A2BRHF", "RA2RHF", "OSNRHF"};
	
	
	private static final Logger logger = LogManager.getLogger(Application.class);
	
	private static String api;
	private static String database_url;
	private static String database_user;
	private static String database_password;
	private static String username;
	private static String password;
	private static String app_id;
	private static String bootstrap_servers;
	private static String schema_registry;
	
	public static void setup() {
		Application.api = System.getenv("API");
		Application.database_url = System.getenv("DATABASE_URL");
		Application.database_user = System.getenv("DATABASE_USER");
		Application.database_password = System.getenv("DATABASE_PASSWORD");
		Application.username = System.getenv("USERNAME");
		Application.password = System.getenv("PASSWORD");
		Application.app_id = System.getenv("app_id");
		Application.bootstrap_servers = System.getenv("BOOTSTRAP_SERVERS");
		Application.schema_registry = System.getenv("SCHEMA_REGISTRY_URL");
	}
	
	public void setup(
			String api, 
			String database_url, 
			String database_user, 
			String database_password, 
			String username, 
			String password, 
			String app_id,
			String bootstrap_servers, 
			String schema_registry
		){
		Application.api = api;
		Application.database_url = database_url;
		Application.database_user = database_user;
		Application.database_password = database_password;
		Application.username = username;
		Application.password = password;
		Application.app_id = app_id;
		Application.bootstrap_servers = bootstrap_servers;
		Application.schema_registry = schema_registry;
	}
	
	public static void main(String[] args) {
		Configurator.initialize(null, "src/main/resources/log4j2.xml");
		setup();
		startApp();
	}
	
	public static void startApp() {
		
		CurrentData currentData = new CurrentData(api, username, password);
		Catalog catalog = new Catalog(api, username, password);
		KiteableWeatherConsumer consumer = new KiteableWeatherConsumer(getProperties(), database_url, database_user, database_password);

		MergedWeatherStream stream = new MergedWeatherStream(app_id, bootstrap_servers, schema_registry);
		
		Thread currentDataThread = new Thread(currentData);
		Thread catalogThread = new Thread(catalog);
		Thread comsumerThread = new Thread(consumer);
		
		Thread streamThread = new Thread(stream);
		
		currentDataThread.start();
		catalogThread.start();
		
		int timeOutException = 1;
		while(currentData.getCurrentDataString() == null || catalog.getCatalogString() == null) {
			if(currentData.getCurrentDataString() == null) {
				logger.info(String.format("ℹ️ retrieving current data - try: %d", timeOutException));
			}
			if(catalog.getCatalogString() == null) {
				logger.info(String.format("ℹ️ Retrieving catalog - try: %d", timeOutException));
			}
			
			try {
				if(timeOutException >= 15) {
					throw new InterruptedException();
				}
				
				timeOutException++;
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("❌ ", e);
				throw new ResourceNotFoundException("URL not found");
			}
			
		};

		
		ApplicationHelper applicationHelper = new ApplicationHelper(currentData, catalog, bootstrap_servers, schema_registry);
		Thread applicationHelperThread = new Thread(applicationHelper);
		
		applicationHelperThread.start();
		streamThread.start();
		comsumerThread.start();
	}
	
	private static Properties getProperties() {
		
        final Properties props = new Properties();
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MVB_consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);   
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
        
        return props;
	}
}
