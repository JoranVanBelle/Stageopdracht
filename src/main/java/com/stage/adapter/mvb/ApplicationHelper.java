package com.stage.adapter.mvb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.producers.RawDataMeasuredProducer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ApplicationHelper extends Thread{


	private static final String[] sensoren = {"MP0WC3", "MP7WC3", "NP7WC3"};
	public static final int CREATE_EVENTS = 1000 * 30 * 01 * 1; // 1000 * 60 * 60 * 1
	private final CurrentData currentData;
	private final Catalog catalog;

	private static final Logger logger = LogManager.getLogger(Application.class);
	
	public ApplicationHelper(CurrentData currentData, Catalog catalog) {
		this.currentData = currentData;
		this.catalog = catalog;
	}
	
	public void run() {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(() -> {
			logger.info("ℹ️ Collecting the current data and catalog");
			JSONObject data = currentData.getCurrentData();
			JSONObject cat = catalog.getCatalog();
			
			for(String sensor : sensoren) {
				String[] params = getParams(cat, data, sensor);
				RawDataMeasuredProducer rdmProd = new RawDataMeasuredProducer(getProperties(), sensor, params[0], params[1], params[2], Long.parseLong(params[3]));
				rdmProd.createEvent();
			}
			
			logger.info(String.format("ℹ️ Raw data saved at %s",  DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now())));
			
		}, 0, CREATE_EVENTS, TimeUnit.MILLISECONDS);
	}
	
private static Properties getProperties() {
		
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MVB_consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        
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
				try {
					SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
					String timestamp = sensorData.getString("Timestamp");
					String dateString = timestamp.split("T")[0];
					String timeZoned = timestamp.split("T")[1];
					String timeString = timeZoned.substring(0, timeZoned.length() - 6);
					Date date;
					date = sdf.parse(String.format("%s %s", dateString, timeString));
					millis = date.getTime();
				} catch (ParseException e) {
					e.printStackTrace();
					
				}
				
				value = String.valueOf(sensorData.getFloat("Value"));
				break;
			}
		}
		
		return new String[] {loc, value, eenheid, Long.toString(millis)};
	}
	
}
