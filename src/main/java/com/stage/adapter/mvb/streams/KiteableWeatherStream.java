package com.stage.adapter.mvb.streams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.stage.*;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import com.stage.adapter.mvb.processors.KiteableWeatherReconProcessor;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWeatherStream extends Thread {

	private static String sensorID;

	private static final String kvStoreNameKiteable = "kiteableWeatherStream";
	private static final String kvStoreNameUnkiteable = "unkiteableWeatherStream";

	private static final String WINDSPEEDTOPIC = "Meetnet.meting.wind.speed.kiteable";
	private static final String WAVETOPIC = "Meetnet.meting.wave.kiteable";
	private static final String WINDDIRECTIONTOPIC = "Meetnet.meting.wind.direction.kiteable";
	private static final String OUTTOPIC = "Meetnet.meting.kiteable";

	private final String app_id;
	private final String bootstrap_servers;
	private final String schema_registry;

	public 	KiteableWeatherStream(String app_id, String bootstrap_servers, String schema_registry ){
		this.app_id = app_id;
		this.bootstrap_servers = bootstrap_servers;
		this.schema_registry = schema_registry;
	}

	@Override
	public void run() {

		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);

		Topology topology = buildTopology(
				WINDSPEEDTOPIC,
				WAVETOPIC,
				WINDDIRECTIONTOPIC,
				OUTTOPIC,
				genericRecordSerde(schema_registry),
				kiteableWeatherDetectedSerde(schema_registry),
				noKiteableWeatherDetectedSerde(schema_registry),
				props
		);

		KafkaStreams streams = new KafkaStreams(topology, props);

		streams.start();
		System.out.println("ℹ️ KiteableWeatherStream started");

	}

	private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {
		Properties settings = new Properties();

		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s.weather", app_id));
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);

		return settings;
	}

	public static GenericAvroSerde genericRecordSerde(String schema_registry) {
		GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		genericAvroSerde.configure(serdeConfig, false);
		return genericAvroSerde;
	}

	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWeatherDetected.configure(serdeConfig, false);
		return kiteableWeatherDetected;
	}

	public static SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		noKiteableWeatherDetected.configure(serdeConfig, false);
		return noKiteableWeatherDetected;
	}

	public static SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWindDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDetectedSerde;
	}

	public static SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		windHasFallenOffSerde.configure(serdeConfig, false);
		return windHasFallenOffSerde;
	}

	public static SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWaveDetectedSerde.configure(serdeConfig, false);
		return kiteableWaveDetectedSerde;
	}

	public static SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		unkiteableWaveDetected.configure(serdeConfig, false);
		return unkiteableWaveDetected;
	}

	public static SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWindDirectionDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDirectionDetectedSerde;
	}

	public static SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		unkiteableWindDirectionDetected.configure(serdeConfig, false);
		return unkiteableWindDirectionDetected;
	}

	protected static Topology buildTopology(
			String windSpeedTopic,
			String waveHeightTopic,
			String windDirectionTopic,
			String outtopic,
			GenericAvroSerde recordSerde,
			SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde,
			SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetectedSerde,
			Properties streamProperties
	) {

		StreamsBuilder builder = new StreamsBuilder();

		builder.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.persistentKeyValueStore(kvStoreNameKiteable),
						Serdes.String(),
						kiteableWeatherDetectedSerde)
		);

		builder.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.persistentKeyValueStore(kvStoreNameUnkiteable),
						Serdes.String(),
						noKiteableWeatherDetectedSerde)
		);

		builder.stream(Arrays.asList(windSpeedTopic, waveHeightTopic, windDirectionTopic), Consumed.with(Serdes.String(), recordSerde))
				.process(() -> new KiteableWeatherReconProcessor(kvStoreNameKiteable, kvStoreNameUnkiteable), kvStoreNameKiteable, kvStoreNameUnkiteable)
				.split()
				.branch((k, v) -> v.getSchema().getName().toString().startsWith("Kiteable"),
						Branched.withConsumer(s -> s
								.mapValues(v -> new KiteableWeatherDetected(String.format("%s%d", v.get("DataID").toString(), (long) v.get("Tijdstip")), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
								.to(outtopic, Produced.with(Serdes.String(), kiteableWeatherDetectedSerde))
						))
				.branch((k, v) -> v.getSchema().getName().startsWith("NoKiteable"),
						Branched.withConsumer(s -> s
								.mapValues(v -> new NoKiteableWeatherDetected(String.format("%s%d", v.get("DataID").toString(), (long) v.get("Tijdstip")), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
								.to(outtopic, Produced.with(Serdes.String(), noKiteableWeatherDetectedSerde))
						));

		return builder.build();
	}
}
