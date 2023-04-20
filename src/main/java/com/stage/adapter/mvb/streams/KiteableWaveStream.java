package com.stage.adapter.mvb.streams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

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

import com.stage.KiteableWaveDetected;
import com.stage.RawDataMeasured;
import com.stage.UnkiteableWaveDetected;
import com.stage.adapter.mvb.processors.KiteableWaveProcessor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWaveStream extends Thread {


	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WAVETOPIC = "Meetnet.meting.wave.kiteable";

	private static final String kvStoreName = "waveStream";
	private static final double threshold = 150;
	private final List<String> SENSOREN = new ArrayList<String>(Arrays.asList(new String[] {"NPBGHA"}));

	private final String app_id;
	private final String bootstrap_servers;
	private final String schema_registry;

	public KiteableWaveStream(String app_id, String bootstrap_servers, String schema_registry) {
		this.app_id = app_id;
		this.bootstrap_servers = bootstrap_servers;
		this.schema_registry = schema_registry;
	}

	@Override
	public void run() {

		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);

		Topology topo = buildTopology(SENSOREN, threshold, INTOPIC, WAVETOPIC, rawDataMeasuredSerde(schema_registry), kiteableWaveDetectedSerde(schema_registry), unkiteableWaveDetectedSerde(schema_registry), streamsConfig(app_id, bootstrap_servers, schema_registry));
		KafkaStreams streams = new KafkaStreams(topo, props);
		streams.start();
		System.out.println("ℹ️ KiteableWaveStream started");

	}

	public static SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde(String schema_registry) {
		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		rawDataMeasuredSerde.configure(serdeConfig, false);
		return rawDataMeasuredSerde;
	}

	public static SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWaveDetectedSerde.configure(serdeConfig, false);
		return kiteableWaveDetectedSerde;
	}

	public static SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		unkiteableWaveDetected.configure(serdeConfig, false);
		return unkiteableWaveDetected;
	}

	private Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {

		Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s.wave.height", app_id));
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);

		return settings;
	}
	
	protected static Topology buildTopology(Collection<String> inScopeSensors,
										  double waveTreshhold,
										  String rawDataTopic,
										  String kiteableWaveTopic,
										  SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde,
										  SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde,
										  SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetectedSerde,
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
			.process(() -> new KiteableWaveProcessor(kvStoreName, waveTreshhold), kvStoreName)
			.split()
			.branch((key, value) -> Double.parseDouble(value.getWaarde()) > waveTreshhold,
					Branched.withConsumer(s -> s
							.mapValues(v -> new KiteableWaveDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
							.peek((k,v) -> {System.out.printf("ℹ️ There is a kiteable wave detected: %s%n", v.getWaarde());})
							.to(kiteableWaveTopic, Produced.with(Serdes.String(), kiteableWaveDetectedSerde))))
			.branch((key, value) -> Double.parseDouble(value.getWaarde()) <= waveTreshhold,
					Branched.withConsumer(s -> s
							.mapValues(v -> new UnkiteableWaveDetected(v.getSensorID(), v.getLocatie(), v.getWaarde(), v.getEenheid(), v.getTijdstip()))
							.peek((k, v) -> {System.out.printf("ℹ️ There is an unkiteable wave detected: %s%n", v.getWaarde());})
							.to(kiteableWaveTopic, Produced.with(Serdes.String(), unkiteableWaveDetectedSerde))));
		
			
			return builder.build(streamProperties);
	}
	
	private static Predicate<String, RawDataMeasured> onlyInScopeSensors(Collection<String> inScopeSensors) {
		return (key_maybe, v) -> Optional.ofNullable(key_maybe).map(inScopeSensors::contains).orElse(false);
	}

}
