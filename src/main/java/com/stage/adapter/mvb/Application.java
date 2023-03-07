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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.producers.RawDataMeasuredProducer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

// doc: https://docs.confluent.io/5.4.1/schema-registry/schema_registry_tutorial.html

@SpringBootApplication
public class Application {

	private static String API = "https://api.meetnetvlaamsebanken.be";
	
	private static final String[] sensoren = {"A2BGHA", "WDLGHA", "RA2GHA", "OSNGHA", "NPBGHA", "SWIGHA",
			"MP0WC3", "MP7WC3", "NP7WC3", "MP0WVC", "MP7WVC", "NP7WVC", "A2BRHF", "RA2RHF", "OSNRHF"};
	public static final int CREATE_EVENTS = 1000 * 60 * 60 * 1;
	
	public static void main(String[] args) {
//		SpringApplication.run(Application.class, args);
		CurrentData currentData = new CurrentData(API);
		Catalog catalog = new Catalog(API);
		
		currentData.run();
		catalog.run();
		
		while(currentData.getCurrentDataString() == null || catalog.getCatalogString() == null) {
			if(currentData.getCurrentDataString() == null) {
				System.out.println("retrieving current data" );
			}
			if(catalog.getCatalogString() == null) {
				System.out.println("retrieving catalog");
			}
		
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		};
		
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(() -> {
			JSONObject data = currentData.getCurrentData();
			JSONObject cat = catalog.getCatalog();
			
			for(String sensor : sensoren) {
				System.out.println(sensor);
				String[] params = getParams(cat, data, sensor);
				RawDataMeasuredProducer rdmProd = new RawDataMeasuredProducer(getProperties(), sensor, params[0], params[1], params[2], Long.parseLong(params[3]));
				rdmProd.createEvent();
			}
			
			System.out.printf("--------------------------------------------------------------------------------Raw data saved at %s--------------------------------------------------------------------------------",  DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()));
			
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
				System.out.println("here");
				break;
			}
		}
		
		return new String[] {loc, value, eenheid, Long.toString(millis)};
	}

}
