package com.stage.adapter.mvb.streams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

import com.stage.KiteableWindDirectionDetected;
import com.stage.RawDataMeasured;
import com.stage.UnkiteableWindDirectionDetected;
import com.stage.adapter.mvb.processors.KiteableWindDirectionProcessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWinddirectionStream {
	
	private static final Logger logger = LogManager.getLogger(KiteableWinddirectionStream.class);
	
	private static final String kvStoreName = "windDirectionStream";
	
//	@Override
//	public void run() {
//		
//		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);
//		
//		Topology topo = buildTopology(threshold, INTOPIC, WINDTOPIC, rawDataMeasuredSerde(props), kiteableWindDirectionDetectedSerde(props), unkiteableWindDirectionDetected(props), props);
//		KafkaStreams streams = new KafkaStreams(topo, props);
//		streams.start();
//		logger.info("ℹ️ KiteableWindDirectionStream started");
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
//	public static SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		kiteableWindDirectionDetectedSerde.configure(serdeConfig, false);
//		return kiteableWindDirectionDetectedSerde;
//	}
//	
//	public static SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected(Properties envProps) {
//		final SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		unkiteableWindDirectionDetected.configure(serdeConfig, false);
//		return unkiteableWindDirectionDetected;
//	}
//	
//private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {
//		
//		Properties settings = new Properties();
//		// Set a few key parameters
//		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, app_id);
//		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
//		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
//		//todo add extra configuration
//
//		// Any further settings
////        settings.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, Optional.ofNullable(System.getenv("QS_SECURITY_PROTOCOL")).orElse("SASL_SSL"));
////        settings.put("sasl.jaas.config", Optional.ofNullable(System.getenv("QS_JAAS_CONFIG")).orElseThrow(() -> new IllegalArgumentException("QS_JAAS_CONFIG is required")));
////        settings.put("ssl.endpoint.identification.algorithm", Optional.ofNullable(System.getenv("QS_ENDPOINT_ID_ALG")).orElse("https"));
////        settings.put("sasl.mechanism", Optional.ofNullable(System.getenv("QS_SASL_MECH")).orElse("PLAIN"));
////        settings.put("replication.factor", Optional.ofNullable(System.getenv("QS_REPLICATION")).orElse("3"));
////        settings.put("auto.offset.reset", Optional.ofNullable(System.getenv("QS_AUTO_OFFSET_RESET")).orElse("earliest"));
//
//		return settings;
//	}

protected static StreamsBuilder buildTopology(Map<String, double[]> thresholds,
								  String rawDataTopic,
								  String kiteableWindDirectionTopic,
								  SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde,
								  SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde,
								  SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetectedSerde,
								  Properties streamProperties,
								  StreamsBuilder builder
){
	
	List<String> inScopeSensors = new ArrayList<>(thresholds.keySet());
	
	builder.addStateStore(
	Stores.keyValueStoreBuilder(
	Stores.persistentKeyValueStore(kvStoreName),
	Serdes.String(),
	rawDataMeasuredSerde)
	);
	
	builder.stream(rawDataTopic, Consumed.with(Serdes.String(), rawDataMeasuredSerde))
	.filter(onlyInScopeSensors(inScopeSensors))
	.process(() -> new KiteableWindDirectionProcessor(kvStoreName, thresholds), kvStoreName)
	.split()
	.branch((key, value) -> isValueBinnenUpperAndLowerBound(value.getWaarde(), thresholds.get(key)[1], thresholds.get(key)[0]),
			Branched.withConsumer(s -> s
					.mapValues(v -> new KiteableWindDirectionDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
    				.peek((k, v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
    				.to(kiteableWindDirectionTopic, Produced.with(Serdes.String(), kiteableWindDirectionDetectedSerde))))
	.branch((key, value) -> !isValueBinnenUpperAndLowerBound(value.getWaarde(), thresholds.get(key)[1], thresholds.get(key)[0]),
			Branched.withConsumer(s -> s
					.mapValues(v -> new UnkiteableWindDirectionDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
    				.peek((k, v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
    				.to(kiteableWindDirectionTopic, Produced.with(Serdes.String(), unkiteableWindDirectionDetectedSerde))));
	
	return builder;
	}

	private static Predicate<String, RawDataMeasured> onlyInScopeSensors(Collection<String> inScopeSensors) {
		return (key_maybe, v) -> Optional.ofNullable(key_maybe).map(inScopeSensors::contains).orElse(false);
	}
	
	private static boolean isValueBinnenUpperAndLowerBound(String value, double upper, double lower) {
		double valueDouble = Double.parseDouble(value);
		return (lower < valueDouble && valueDouble < upper);
	}
}
