package com.stage.adapter.mvb.streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KitableWindDetected;
import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.processors.KitableWindProcessor;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

// https://stackoverflow.com/questions/58745670/kafka-compare-consecutive-values-for-a-key

public class KitableWindStream extends Thread {

	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WINDTOPIC = "Meetnet.meting.wind.kitable";
	private final List<String> SENSOREN = new ArrayList<String>(Arrays.asList(new String[] {"NP7WC3"}));

	private static final Logger logger = LogManager.getLogger(KitableWindStream.class);

	@Override
	public void run() {
		final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url","http://localhost:8081");
		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
		rawDataMeasuredSerde.configure(serdeConfig, false);
        final SpecificAvroSerde<KitableWindDetected> kitableWindDetectedSerde = new SpecificAvroSerde<>();
        kitableWindDetectedSerde.configure(serdeConfig, false);
        
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, RawDataMeasured> rawDataMeasuredStream = builder.stream(INTOPIC, Consumed.with(Serdes.String(), rawDataMeasuredSerde));
        
        builder.addStateStore(
        		Stores.keyValueStoreBuilder(
        				Stores.persistentKeyValueStore("most-recent-event"),
        				Serdes.String(),
        				rawDataMeasuredSerde)
        		);
        
        try {
            rawDataMeasuredStream
	            .peek((k,v) -> System.err.println("here"))
	        	.filter((k,v) -> this.SENSOREN.contains(k))
	        	.mapValues(v -> new KitableWindDetected(v.getSensorID(), v.getLocatie(), Float.parseFloat(v.getWaarde()) > 7.717 , v.getTijdstip()))
//	        	.process(KitableWindProcessor::new, "most-recent-event")
	        	.peek((k, v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
	        	.to(WINDTOPIC, Produced.with(Serdes.String(), kitableWindDetectedSerde));
        
            Properties props = getProperties();
			KafkaStreams streams = new KafkaStreams(builder.build(), props);
			streams.start();
	        logger.info("ℹ️ KitableWindStream started");
	        
        } catch(Exception e) {
        	e.printStackTrace();
        }

	}
	
	private static Properties getProperties() {
		
		Properties props = new Properties();
		
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, KitableWindStream.class.toString());
		
		props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
		props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "snappy");
		props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 3);
		props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 500);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
	        
		
        
        
        return props;
	}
	
}