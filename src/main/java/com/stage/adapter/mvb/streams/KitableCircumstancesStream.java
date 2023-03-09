package com.stage.adapter.mvb.streams;

import java.util.Collections;
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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KitableCircumstancesDetected;
import com.stage.KitableWaveDetected;
import com.stage.KitableWindDetected;
import com.stage.adapter.mvb.processors.KitableWaveProcessor;
import com.stage.adapter.mvb.processors.ReconcilationProcessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KitableCircumstancesStream extends Thread {

	private static final String WINDTOPIC = "Meetnet.meting.wind.kitable";
	private static final String WAVETOPIC = "Meetnet.meting.wave.kitable";
	private static final String KITETOPIC = "Meetnet.meting.kitable";
	
	private static final Logger logger = LogManager.getLogger(KitableCircumstancesStream.class);
	
	@Override
	public void run() {
		final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url","http://localhost:8081");
		final SpecificAvroSerde<KitableWindDetected> kitableWindDetectedSerde = new SpecificAvroSerde<>();
		kitableWindDetectedSerde.configure(serdeConfig, false);
		final SpecificAvroSerde<KitableWaveDetected> kitableWaveDetectedSerde = new SpecificAvroSerde<>();
		kitableWaveDetectedSerde.configure(serdeConfig, false);
        final SpecificAvroSerde<KitableCircumstancesDetected> kitableCircumstancesDetectedSerde = new SpecificAvroSerde<>();
        kitableCircumstancesDetectedSerde.configure(serdeConfig, false);
        
        StreamsBuilder builder = new StreamsBuilder();
        
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("merge_store"), Serdes.String(), kitableCircumstancesDetectedSerde));
        
        KStream<String, KitableWindDetected> kitableWindDetectedStream = builder.stream(WINDTOPIC, Consumed.with(Serdes.String(), kitableWindDetectedSerde))
        		.selectKey((k,v) -> v.getSensorID())
        		.repartition(Repartitioned.with(Serdes.String(), kitableWindDetectedSerde).withName("KitableWind_byDataId"));
        
        KStream<String, KitableWaveDetected> kitableWaveDetectedStream = builder.stream(WAVETOPIC, Consumed.with(Serdes.String(), kitableWaveDetectedSerde))
        		.selectKey((k,v) -> v.getSensorID())
        		.repartition(Repartitioned.with(Serdes.String(), kitableWaveDetectedSerde).withName("KitableWave_byDataId"));
        
        kitableWindDetectedStream.process(ReconcilationProcessor::new, "merge_store")
        	.merge(kitableWaveDetectedStream.process(KitableWaveProcessor::new, "merge_store"))
        	.to(KITETOPIC, Produced.with(Serdes.String(), kitableCircumstancesDetectedSerde));
        
		KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
		streams.start();
        logger.info("ℹ️ KitableCircumstancesStream started");
	}
	
	private Properties getProperties() {
		
		Properties props = new Properties();
		
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
