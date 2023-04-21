package com.stage.adapter.mvb.streams;

import java.util.*;

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

import com.stage.KiteableWindDirectionDetected;
import com.stage.RawDataMeasured;
import com.stage.UnkiteableWindDirectionDetected;
import com.stage.adapter.mvb.processors.KiteableWindDirectionProcessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.maven.wagon.Streams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class KiteableWinddirectionStream extends Thread {
	
	private static final String kvStoreName = "windDirectionStream";

	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WINDTOPIC = "Meetnet.meting.wind.direction.kiteable";


		private final Map<String, double[]> threshold  = new HashMap<>() {{
			put("NP7WC3", new double[] {10.00, 230.00});
		}};

	private final String app_id;
	private final String bootstrap_servers;
	private final String schema_registry;

	public KiteableWinddirectionStream(String app_id, String bootstrap_servers, String schema_registry) {
		this.app_id = app_id;
		this.bootstrap_servers = bootstrap_servers;
		this.schema_registry = schema_registry;
	}

	@Override
	public void run() {

		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);

		Topology topo = buildTopology(threshold, INTOPIC, WINDTOPIC, rawDataMeasuredSerde(schema_registry), kiteableWindDirectionDetectedSerde(schema_registry), unkiteableWindDirectionDetected(schema_registry), props);
		KafkaStreams streams = new KafkaStreams(topo, props);
		streams.start();
		System.out.println("ℹ️ KiteableWindDirectionStream started");

	}

	public static SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde(String schema_registry) {
		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		rawDataMeasuredSerde.configure(serdeConfig, false);
		return rawDataMeasuredSerde;
	}

	public static SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWindDirectionDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDirectionDetectedSerde;
	}

	public static SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		unkiteableWindDirectionDetected.configure(serdeConfig, false);
		return unkiteableWindDirectionDetected;
	}

private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {

		Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s.wind.direction", app_id));
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		settings.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		return settings;
	}

protected static Topology buildTopology(Map<String, double[]> thresholds,
										String rawDataTopic,
										String kiteableWindDirectionTopic,
										SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde,
										SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde,
										SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetectedSerde,
										Properties streamProperties
){
	StreamsBuilder builder = new StreamsBuilder();
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
    				.peek((k, v) -> {System.out.printf("➡️ There is a kiteable winddirection detected: %s%n", v.getWaarde());})
    				.to(kiteableWindDirectionTopic, Produced.with(Serdes.String(), kiteableWindDirectionDetectedSerde))))
	.branch((key, value) -> !isValueBinnenUpperAndLowerBound(value.getWaarde(), thresholds.get(key)[1], thresholds.get(key)[0]),
			Branched.withConsumer(s -> s
					.mapValues(v -> new UnkiteableWindDirectionDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
    				.peek((k, v) -> {System.out.printf("➡️ There is an unkiteable winddirection detected: %s%n", v.getWaarde());})
    				.to(kiteableWindDirectionTopic, Produced.with(Serdes.String(), unkiteableWindDirectionDetectedSerde))));
	
	return builder.build(streamProperties);
	}

	private static Predicate<String, RawDataMeasured> onlyInScopeSensors(Collection<String> inScopeSensors) {
		return (key_maybe, v) -> Optional.ofNullable(key_maybe).map(inScopeSensors::contains).orElse(false);
	}
	
	private static boolean isValueBinnenUpperAndLowerBound(String value, double upper, double lower) {
		double valueDouble = Double.parseDouble(value);
		return (lower < valueDouble && valueDouble < upper);
	}
}
