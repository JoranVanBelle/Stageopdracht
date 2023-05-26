package com.stage.adapter.mvb.helpers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

//import com.stage.adapter.mvb.Application;
import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.producers.RawDataMeasuredProducer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ApplicationHelper extends Thread{


	private static final String[] sensoren = {"MP0WC3", "MP7WC3", "NP7WC3", "NPBGHA", "NP7WVC"};
	public static final int CREATE_EVENTS = 10;
	private final CurrentData currentData;
	private final Catalog catalog;
	private final String bootstrap_servers;
	private final String schema_registry;
	private final String userInfo;
	private final String credentialsSource;
	private final int timeoutMs;
	private final String dnsLookup;
	private final String saslMechanism;
	private final String saslJaasConfig;
	private final String securityProtocol;
	
	public ApplicationHelper(
			CurrentData currentData, 
			Catalog catalog,
			String bootstrap_servers,
			String schema_registry, 
			String userInfo, String credentialsSource, 
			int timeoutMs, String dnsLookup, 
			String saslMechanism, String saslJaasConfig, 
			String securityProtocol
		) {
		this.currentData = currentData;
		this.catalog = catalog;
		this.bootstrap_servers = bootstrap_servers;
		this.schema_registry = schema_registry;
		this.userInfo = userInfo;
		this.credentialsSource = credentialsSource;
		this.timeoutMs = timeoutMs;
		this.dnsLookup = dnsLookup;
		this.saslMechanism = saslMechanism;
		this.saslJaasConfig = saslJaasConfig;
		this.securityProtocol = securityProtocol;
	}
	
	@Override
	public void run() {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(() -> {
			System.out.printf("ℹ️ Collecting the current data and catalog - %s\r\n", DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()));
			JSONObject data = currentData.getCurrentData();
			JSONObject cat = catalog.getCatalog();

			System.out.println(cat);
			System.out.println(data);

			for(String sensor : sensoren) {
				String[] params = getParams(cat, data, sensor);
				RawDataMeasuredProducer rdmProd = new RawDataMeasuredProducer(getProperties(bootstrap_servers, schema_registry, userInfo, credentialsSource, timeoutMs,
						dnsLookup, saslMechanism, saslJaasConfig, securityProtocol), sensor, params[0], params[1], params[2], Long.parseLong(params[3]));
				rdmProd.createEvent();
			}
			
		}, 0, CREATE_EVENTS, TimeUnit.MINUTES);
	}
	
private static Properties getProperties(String bootstrap_servers, String schema_registry, String userInfo, String credentialsSource, int timeoutMs,
		String dnsLookup, String saslMechanism, String saslJaasConfig, String securityProtocol) {
		
        final Properties props = new Properties();
        
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("basic.auth.user.info", userInfo);
        props.put("basic.auth.credentials.source", credentialsSource);
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
		
		props.put("session.timeout.ms", timeoutMs);
		props.put("client.dns.lookup", dnsLookup);
		props.put("sasl.mechanism", saslMechanism);
		props.put("sasl.jaas.config", saslJaasConfig);
		props.put("security.protocol", securityProtocol);
        
        return props;
	}
	
	private static String[] getParams(JSONObject cat, JSONObject currentData, String sensor) {
		String loc = null;
		String eenheid = null;
		long millis= 0L;
		String value = null;
		JSONArray locations = cat.getJSONArray("Locations");
		for (int i = 0; i < locations.length(); i++) {
            JSONObject location = locations.getJSONObject(i);
            if (location.getString("ID").equals(sensor.substring(0,sensor.length() - 3))) {
                JSONArray names = location.getJSONArray("Name");
                JSONObject name = names.getJSONObject(0);
                loc = name.getString("Message");
                break;
            }
        }
		JSONArray parameters = cat.getJSONArray("Parameters");

		for (int i = 0; i < parameters.length(); i++) {
		    JSONObject parameter = parameters.getJSONObject(i);
		    String id = parameter.getString("ID");
		    if (id.equals(sensor.substring(sensor.length()-3))) {
		        eenheid = parameter.getString("Unit");
		        break;
		    }
		}

		JSONArray data = currentData.getJSONArray("current data");
		for (int i = 0; i < data.length(); i++) {
			JSONObject sensorData = data.getJSONObject(i);
			if (sensorData.getString("ID").equals(sensor)) {
		        Instant instant = Instant.parse(sensorData.getString("Timestamp"));
		        millis = instant.toEpochMilli();
				
				value = String.valueOf(sensorData.getFloat("Value"));
				break;
			}
		}

		return new String[] {loc, value, eenheid, Long.toString(millis)};
	}
	
}
