package com.stage.adapter.mvb.streams;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KiteableWaveDetected;
import com.stage.RawDataMeasured;
import com.stage.UnkiteableWaveDetected;
import com.stage.adapter.mvb.processors.KiteableWaveProcessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWaveStream {
	
	private static final Logger logger = LogManager.getLogger(KiteableWaveStream.class);
	
	private static final String kvStoreName = "waveStream";
	private static final double threshold = 150;
//	
//	@Override
//	public void run() {
//		
//		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);
//		
//		Topology topo = buildTopology(SENSOREN, threshold, INTOPIC, WAVETOPIC, rawDataMeasuredSerde(props), kiteableWaveDetectedSerde(props), unkiteableWaveDetectedSerde(props), props);
//		KafkaStreams streams = new KafkaStreams(topo, props);
//		streams.start();
//		logger.info("ℹ️ KiteableWaveStream started");
//		
//		GracefulShutdown.gracefulShutdown(streams);
//
//	}
//	
//	public static SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde(Properties envProps) {
//		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		rawDataMeasuredSerde.configure(serdeConfig, false);
//		return rawDataMeasuredSerde;
//	}
//
//	public static SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		kiteableWaveDetectedSerde.configure(serdeConfig, false);
//		return kiteableWaveDetectedSerde;
//	}
//	
//	public static SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetected = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		unkiteableWaveDetected.configure(serdeConfig, false);
//		return unkiteableWaveDetected;
//	}
//	
//	private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {
//		
//		Properties settings = new Properties();
//		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, app_id);
//		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
//		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
//
//		return settings;
//	}
	
	protected static StreamsBuilder buildTopology(Collection<String> inScopeSensors,
										  double windspeedTreshhold,
										  String rawDataTopic,
										  String kiteableWaveTopic,
										  SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde,
										  SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde,
										  SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetectedSerde,
										  Properties streamProperties,
										  StreamsBuilder builder
	){
		
		builder.addStateStore(
		Stores.keyValueStoreBuilder(
		Stores.persistentKeyValueStore(kvStoreName),
		Serdes.String(),
		rawDataMeasuredSerde)
		);
			
		builder.stream(rawDataTopic, Consumed.with(Serdes.String(), rawDataMeasuredSerde))
			.filter(onlyInScopeSensors(inScopeSensors))
			.process(() -> new KiteableWaveProcessor(kvStoreName, threshold), kvStoreName)
			.split()
			.branch((key, value) -> Double.parseDouble(value.getWaarde()) > threshold,
					Branched.withConsumer(s -> s
							.mapValues(v -> new KiteableWaveDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
							.peek((k,v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
							.to(kiteableWaveTopic, Produced.with(Serdes.String(), kiteableWaveDetectedSerde))))
			.branch((key, value) -> Double.parseDouble(value.getWaarde()) <= threshold,
					Branched.withConsumer(s -> s
							.mapValues(v -> new UnkiteableWaveDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
							.peek((k, v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
							.to(kiteableWaveTopic, Produced.with(Serdes.String(), unkiteableWaveDetectedSerde))));
		
			
			return builder;
	}
	
	private static Predicate<String, RawDataMeasured> onlyInScopeSensors(Collection<String> inScopeSensors) {
		return (key_maybe, v) -> Optional.ofNullable(key_maybe).map(inScopeSensors::contains).orElse(false);
	}
}
