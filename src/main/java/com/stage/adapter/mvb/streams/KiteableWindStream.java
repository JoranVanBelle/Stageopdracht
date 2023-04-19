package com.stage.adapter.mvb.streams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.*;

import com.stage.adapter.mvb.processors.KiteableWindSpeedProcessor;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KiteableWindDetected;
import com.stage.RawDataMeasured;
import com.stage.UnkiteableWindDetected;
import com.stage.adapter.mvb.processors.KiteableWaveProcessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

// https://stackoverflow.com/questions/58745670/kafka-compare-consecutive-values-for-a-key

public class KiteableWindStream extends Thread {


	private static final Logger logger = LogManager.getLogger(KiteableWindStream.class);
	
	private static final String kvStoreName = "windStream";
	private static final double threshold = 7.717;
	private final List<String> SENSOREN = new ArrayList<String>(Arrays.asList(new String[] {"NP7WVC"}));

	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WINDTOPIC = "Meetnet.meting.wind.speed.kiteable";

	private final String app_id;
	private final String bootstrap_servers;
	private final String schema_registry;

	public KiteableWindStream(String app_id, String bootstrap_servers, String schema_registry){
		this.app_id = app_id;
		this.bootstrap_servers = bootstrap_servers;
		this.schema_registry = schema_registry;
	}

	@Override
	public void run() {
		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);
		Topology topo = buildTopology(SENSOREN, threshold, INTOPIC, WINDTOPIC, rawDataMeasuredSerde(schema_registry), kiteableWindDetectedSerde(schema_registry), windHasFallenOffSerde(schema_registry), streamsConfig(app_id, bootstrap_servers, schema_registry));
		KafkaStreams streams = new KafkaStreams(topo, props);
		streams.start();
		System.out.println("ℹ️ KiteableWindStream started");
	}

	public static SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde(String schema_registry) {
		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		rawDataMeasuredSerde.configure(serdeConfig, false);
		return rawDataMeasuredSerde;
	}

	public static SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWindDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDetectedSerde;
	}

	public static SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		windHasFallenOffSerde.configure(serdeConfig, false);
		return windHasFallenOffSerde;
	}

	private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {
		Properties settings = new Properties();
		// Set a few key parameters
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s.wind.speed", app_id));
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		return settings;
	}

	protected static Topology buildTopology(Collection<String> inScopeSensors,
										  double windspeedTreshold,
										  String rawDataTopic,
										  String kiteableWindTopic,
										  SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde,
										  SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde,
										  SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde,
										  Properties streamProperties
	){
		StreamsBuilder builder = new StreamsBuilder();

		builder.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.persistentKeyValueStore(kvStoreName),
						Serdes.String(),
						rawDataMeasuredSerde)
		);
		
		builder.stream(rawDataTopic, Consumed.with(Serdes.String(), rawDataMeasuredSerde))
	        .filter(onlyInScopeSensors(inScopeSensors))
	        .process(()-> new KiteableWindSpeedProcessor(kvStoreName, windspeedTreshold), kvStoreName)
	        .split()
	        .branch((key,value) -> Double.parseDouble(value.getWaarde()) > windspeedTreshold,
	        		Branched.withConsumer(s -> s
	        				.mapValues(v -> new KiteableWindDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
	        				.peek((k, v) -> {System.out.printf("ℹ️ There is a kiteable windspeed detected: %s%n", v.getWaarde());})
	        				.to(kiteableWindTopic, Produced.with(Serdes.String(), kiteableWindDetectedSerde))))
	        
	        .branch((key,value) -> Double.parseDouble(value.getWaarde()) <= windspeedTreshold,
	        		Branched.withConsumer(s -> s
	        		.mapValues(v -> new UnkiteableWindDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
    				.peek((k, v) -> {System.out.printf("ℹ️ There is an unkiteable windspeed detected: %s%n", v.getWaarde());})
	        		.to(kiteableWindTopic, Produced.with(Serdes.String(), windHasFallenOffSerde))));

		return builder.build(streamProperties);
	}

	private static Predicate<String, RawDataMeasured> onlyInScopeSensors(Collection<String> inScopeSensors) {
		return (key_maybe, v) -> Optional.ofNullable(key_maybe).map(inScopeSensors::contains).orElse(false);
	}
	
}
