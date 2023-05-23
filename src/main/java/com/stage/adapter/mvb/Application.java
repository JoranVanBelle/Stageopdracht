package com.stage.adapter.mvb;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.stage.adapter.mvb.consumers.KiteableWeatherConsumer;
import com.stage.adapter.mvb.helpers.ApplicationHelper;
import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.streams.KiteableWaveStream;
import com.stage.adapter.mvb.streams.KiteableWeatherStream;
import com.stage.adapter.mvb.streams.KiteableWindStream;
import com.stage.adapter.mvb.streams.KiteableWinddirectionStream;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class Application {

	private static String api;
	private static String database_url;
	private static String database_user;
	private static String database_password;
	private static String username;
	private static String password;
	private static String app_id;
	private static String bootstrap_servers;
	private static String schema_registry;
	private static String emailHost;
	private static int emailPort;
	private static String email_username;
	private static String email_password;
	private static String baseUrl;

	public static void setup() {
		Application.api = System.getenv("API");
		Application.database_url = System.getenv("DATABASE_URL");
		Application.database_user = System.getenv("DATABASE_USER");
		Application.database_password = System.getenv("DATABASE_PASSWORD");
		Application.username = System.getenv("USERNAME");
		Application.password = System.getenv("PASSWORD");
		Application.app_id = System.getenv("APP_ID");
		Application.bootstrap_servers = System.getenv("BOOTSTRAP_SERVERS");
		Application.schema_registry = System.getenv("SCHEMA_REGISTRY_URL");
		Application.emailHost = System.getenv("EMAILHOST");
		Application.emailPort = Integer.parseInt(System.getenv("EMAILPORT"));
		Application.email_username = System.getenv("API_USERNAME");
		Application.email_password = System.getenv("API_PASSWORD");
		Application.baseUrl = System.getenv("BASEURL");
	}

	public static void setup(String api, String database_url, String database_user, String database_password, String username,
							 String password, String app_id, String bootstrap_servers, String schema_registry, String emailHost, int emailPort, String email_username, String email_password, String baseUrl) {
		Application.api = api;
		Application.database_url = database_url;
		Application.database_user = database_user;
		Application.database_password = database_password;
		Application.username = username;
		Application.password = password;
		Application.app_id = app_id;
		Application.bootstrap_servers = bootstrap_servers;
		Application.schema_registry = schema_registry;
		Application.emailHost = emailHost;
		Application.emailPort = emailPort;
		Application.email_username = email_username;
		Application.email_password = email_password;
		Application.baseUrl = baseUrl;
	}

	public static void main(String[] args) {
		setup();
		startApp();
	}

	public static void startApp() {

		CurrentData currentData = new CurrentData(api, username, password);
		Catalog catalog = new Catalog(api, username, password);
		KiteableWeatherConsumer consumer = new KiteableWeatherConsumer(getProperties(), database_url, database_user,
				database_password, emailHost, emailPort, email_username, email_password, baseUrl);

		KiteableWaveStream waveStream = new KiteableWaveStream(app_id, bootstrap_servers, schema_registry);
		KiteableWindStream windspeedStream = new KiteableWindStream(app_id, bootstrap_servers, schema_registry);
		KiteableWinddirectionStream winddirectionStream = new KiteableWinddirectionStream(app_id, bootstrap_servers, schema_registry);
		KiteableWeatherStream weatherStream = new KiteableWeatherStream(app_id, bootstrap_servers, schema_registry);

		Thread currentDataThread = new Thread(currentData);
		Thread catalogThread = new Thread(catalog);
		Thread comsumerThread = new Thread(consumer);

		Thread waveStreamThread = new Thread(waveStream);
		Thread windspeedStreamThread = new Thread(windspeedStream);
		Thread winddirectionStreamThread = new Thread(winddirectionStream);
		Thread weatherStreamThread = new Thread(weatherStream);

		currentDataThread.start();
		catalogThread.start();

		int timeOutException = 1;
		while (currentData.getCurrentDataString() == null || catalog.getCatalogString() == null) {
			if (currentData.getCurrentDataString() == null) {
				System.out.println(String.format("ℹ️ retrieving current data - try: %d/15", timeOutException));
			}
			if (catalog.getCatalogString() == null) {
				System.out.println(String.format("ℹ️ Retrieving catalog - try: %d/15", timeOutException));
			}

			try {
				if (timeOutException >= 15) {
					throw new InterruptedException();
				}

				timeOutException++;
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.err.printf("❌ %s", e);
				throw new ResourceNotFoundException("URL not found");
			}

		}

		ApplicationHelper applicationHelper = new ApplicationHelper(currentData, catalog, bootstrap_servers,
				schema_registry);
		Thread applicationHelperThread = new Thread(applicationHelper);

		applicationHelperThread.start();
		waveStreamThread.start();
		windspeedStreamThread.start();
		winddirectionStreamThread.start();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		weatherStreamThread.start();
		comsumerThread.start();
	}

	public static Properties getProperties() {
		final Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, app_id);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);

		return props;
	}
}
