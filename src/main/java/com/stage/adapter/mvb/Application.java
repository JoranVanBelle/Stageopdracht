package com.stage.adapter.mvb;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import com.stage.adapter.mvb.helpers.ApplicationHelper;
import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.streams.KiteableWaveStream;
import com.stage.adapter.mvb.streams.KiteableWindStream;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class Application {

	private static String API = "https://api.meetnetvlaamsebanken.be";

//	private static final String[] sensoren = {"A2BGHA", "WDLGHA", "RA2GHA", "OSNGHA", "NPBGHA", "SWIGHA",
//			"MP0WC3", "MP7WC3", "NP7WC3", "MP0WVC", "MP7WVC", "NP7WVC", "A2BRHF", "RA2RHF", "OSNRHF"};
	
	
	private static final Logger logger = LogManager.getLogger(Application.class);
	
	
	public static void main(String[] args) {
		Configurator.initialize(null, "src/main/resources/log4j2.xml");
		
		CurrentData currentData = new CurrentData(API);
		Catalog catalog = new Catalog(API);
		KiteableWindStream kitableWindStream = new KiteableWindStream();
		KiteableWaveStream kitableWaveStream = new KiteableWaveStream();

		
		Thread currentDataThread = new Thread(currentData);
		Thread catalogThread = new Thread(catalog);
		Thread windStreamThread = new Thread(kitableWindStream);
		Thread waveStreamThread = new Thread(kitableWaveStream);
		
		currentDataThread.start();
		catalogThread.start();
		
		
		while(currentData.getCurrentDataString() == null || catalog.getCatalogString() == null) {
			if(currentData.getCurrentDataString() == null) {
				logger.info("ℹ️ retrieving current data" );
			}
			if(catalog.getCatalogString() == null) {
				logger.info("ℹ️ Retrieving catalog");
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("❌ ", e);
			}
			
		};

		
		ApplicationHelper applicationHelper = new ApplicationHelper(currentData, catalog);
		Thread applicationHelperThread = new Thread(applicationHelper);
		
		applicationHelperThread.start();
		windStreamThread.start();
		waveStreamThread.start();
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
        
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KitableCircumstances");

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "snappy");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 3);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 500);
        
        
        return props;
	}

}
