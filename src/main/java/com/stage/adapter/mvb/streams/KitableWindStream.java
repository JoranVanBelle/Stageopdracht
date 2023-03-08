package com.stage.adapter.mvb.streams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KitableWindDetected;
import com.stage.RawDataMeasured;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KitableWindStream extends Thread{

	private final Properties props;
	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WINDTOPIC = "Meetnet.meting.wind.kitable";
	private final List<String> SENSOREN = new ArrayList<String>(Arrays.asList(new String[] {"NP7WC3"}));
	

	private static final Logger logger = LogManager.getLogger(KitableWindStream.class);
	
	public KitableWindStream(Properties props) {
		this.props = settings(props);
	}

	@Override
	public void run() {
		final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url","http://localhost:8081");
		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
		rawDataMeasuredSerde.configure(serdeConfig, false);
        final SpecificAvroSerde<KitableWindDetected> kitableWindDetectedSerde = new SpecificAvroSerde<>();
        kitableWindDetectedSerde.configure(serdeConfig, false);
        
        KafkaConsumer<String, RawDataMeasured> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(INTOPIC));
        
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, RawDataMeasured> rawDataMeasuredStream = builder.stream(INTOPIC, Consumed.with(Serdes.String(), rawDataMeasuredSerde));
        
        rawDataMeasuredStream
        	.filter((k,v) -> this.SENSOREN.contains(k))
        	.mapValues(v -> new KitableWindDetected(v.getSensorID(), v.getLocatie(), Float.parseFloat(v.getWaarde()) > 7.717 , v.getTijdstip()))
//        	.peek((k, v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
        	.to(WINDTOPIC, Produced.with(Serdes.String(), kitableWindDetectedSerde));
        
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
		
		while(true) {
		    ConsumerRecords<String, RawDataMeasured> records = consumer.poll(Duration.ofMillis(100));
		    logger.info("⚠️ Looking for events");
		    if (!records.isEmpty()) {
				streams.close();
				streams = new KafkaStreams(builder.build(), props);
		        streams.start();
		        logger.info("ℹ️ Streaming ended");
		    }
		}
		
	}
	
	private Properties settings(Properties settings) {
	    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
	    settings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
	    settings.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
	    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "Wavestream");

	    settings.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
	    settings.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "snappy");
	    settings.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 3);
	    settings.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 500);

	    return settings;
	}
	
}
