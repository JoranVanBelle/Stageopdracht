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
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

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
	
	private static String cluster_api_key;
	private static String cluster_api_secret;
	private static String sr_api_key;
	private static String sr_api_secret;
	
	private static String userInfo;
	private static String credentialsSource;
	private static String resetConfig;
	private static int timeoutMs;
	private static String dnsLookup;
	private static String saslMechanism;
	private static String saslJaasConfig;
	private static String securityProtocol;
	
	public static void setup() {
		Application.api = System.getenv("API");
		Application.database_url = String.format("jdbc:postgresql:///%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=%s&password=%s", System.getenv("DATABASE_NAME"), System.getenv("DATABASE_INSTANCE_CONNECTION_NAME"), System.getenv("DATABASE_USERNAME"), System.getenv("DATABASE_PASSWORD"));
		Application.database_user = System.getenv("DATABASE_USER");
		Application.database_password = System.getenv("DATABASE_PASSWORD");
		Application.username = System.getenv("USERNAME");
		Application.password = System.getenv("PASSWORD");
		Application.app_id = System.getenv("APP_ID");
		Application.bootstrap_servers = System.getenv("BOOTSTRAP_SERVERS");
		Application.schema_registry = System.getenv("SCHEMA_REGISTRY_URL");
		Application.emailHost = System.getenv("EMAILHOST");
		Application.emailPort = Integer.parseInt(System.getenv("EMAILPORT"));
		Application.email_username = System.getenv("EMAIL_USERNAME");
		Application.email_password = System.getenv("EMAIL_PASSWORD");
		Application.baseUrl = System.getenv("BASEURL");
		
		Application.cluster_api_key = System.getenv("CLUSTER_API_KEY");
		Application.cluster_api_secret = System.getenv("CLUSTER_API_SECRET");
		Application.sr_api_key = System.getenv("SR_API_KEY");
		Application.sr_api_secret = System.getenv("SR_API_SECRET");
		
		Application.userInfo = String.format("%s:%s", sr_api_key, sr_api_secret);
		Application.credentialsSource = System.getenv("CREDENTIAL_SOURCE");
		Application.resetConfig = System.getenv("RESET_CONFIG");
		Application.timeoutMs = Integer.parseInt(System.getenv("TIMEOUT_MS"));
		Application.dnsLookup = System.getenv("DNS_LOOKUP");
		Application.saslMechanism = System.getenv("SASL_MECHANISM");
		Application.saslJaasConfig =  String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", cluster_api_key, cluster_api_secret);
		Application.securityProtocol = System.getenv("SECURITY_PROTOCOL");
	}

	// test purpose
	public static void setup(String api, String database_url, String database_user, String database_password, String username,
							 String password, String app_id, String bootstrap_servers, String schema_registry, String emailHost, 
							 int emailPort, String email_username, String email_password, String baseUrl,
							 String cluster_api_key, String cluster_api_secret, String sr_api_key, String sr_api_secret,
							 String userInfo, String credentialsSource, String resetConfig, int timeoutMs, String dnsLookup, 
							 String saslMechanism, String saslJaasConfig, String securityProtocol) {
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
		
		Application.cluster_api_key = cluster_api_key;
		Application.cluster_api_secret = cluster_api_secret;
		Application.sr_api_key = sr_api_key;
		Application.sr_api_secret = sr_api_secret;
		
		Application.userInfo = userInfo;
		Application.credentialsSource = credentialsSource;
		Application.resetConfig = resetConfig;
		Application.timeoutMs = timeoutMs;
		Application.dnsLookup = dnsLookup;
		Application.saslMechanism = saslMechanism;
		Application.saslJaasConfig =  saslJaasConfig;
		Application.securityProtocol = securityProtocol;
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

		KiteableWaveStream waveStream = new KiteableWaveStream(app_id, bootstrap_servers, schema_registry, saslJaasConfig, saslMechanism, resetConfig, securityProtocol);
		KiteableWindStream windspeedStream = new KiteableWindStream(app_id, bootstrap_servers, schema_registry, saslJaasConfig, saslMechanism, resetConfig, securityProtocol);
		KiteableWinddirectionStream winddirectionStream = new KiteableWinddirectionStream(app_id, bootstrap_servers, schema_registry, saslJaasConfig, saslMechanism, resetConfig, securityProtocol);
		KiteableWeatherStream weatherStream = new KiteableWeatherStream(app_id, bootstrap_servers, schema_registry, saslJaasConfig, saslMechanism, resetConfig, securityProtocol);

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
				System.err.println("URL not found");
			}

		}

		ApplicationHelper applicationHelper = new ApplicationHelper(currentData, catalog, bootstrap_servers,
				schema_registry, userInfo, credentialsSource, timeoutMs, dnsLookup, saslMechanism, saslJaasConfig, securityProtocol);
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

		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put("basic.auth.user.info", userInfo);
		props.put("basic.auth.credentials.source", credentialsSource);
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, app_id);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetConfig);
		
		props.put("session.timeout.ms", timeoutMs);
		props.put("client.dns.lookup", dnsLookup);
		props.put("sasl.mechanism", saslMechanism);
		props.put("sasl.jaas.config", saslJaasConfig);
		props.put("security.protocol", securityProtocol);
		
		return props;
	}
}
