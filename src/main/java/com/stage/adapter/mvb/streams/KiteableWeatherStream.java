package com.stage.adapter.mvb.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.adapter.mvb.processors.KiteableWeatherReconProcessor;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWeatherStream {
	
	private static final String kvStoreNameKiteable = "kiteableWeatherStream";
	private static final String kvStoreNameUnkiteable = "unkiteableWeatherStream";
	
//	@Override
//	public void run() {
//		Properties props = streamsConfig(app_id, bootstrap_servers, schema_registry);
//		
//		Topology topology = buildTopology(
//				WINDSPEEDTOPIC, 
//				WAVETOPIC, 
//				WINDDIRECTIONTOPIC, 
//				OUTTOPIC, 
//				genericRecordSerde(props), 
//				kiteableWeatherDetectedSerde(props), 
//				noKiteableWeatherDetectedSerde(props),
//				props
//		);
//		
//		KafkaStreams streams = new KafkaStreams(topology, props);
//		
//		streams.start();
//		logger.info("ℹ️ KiteableWeatherStream started");
//		
//		GracefulShutdown.gracefulShutdown(streams);
//				
//	}
//	
//	private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {
//		Properties settings = new Properties();
//
//		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, app_id);
//		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
//		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
//
//		return settings;
//	}
//	
//	public static GenericAvroSerde genericRecordSerde(Properties envProps) {
//		GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		genericAvroSerde.configure(serdeConfig, false);
//		return genericAvroSerde;
//	}
//	
//	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		kiteableWeatherDetected.configure(serdeConfig, false);
//		return kiteableWeatherDetected;
//	}
//	
//	public static SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetected = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		noKiteableWeatherDetected.configure(serdeConfig, false);
//		return noKiteableWeatherDetected;
//	}
//	
//	public static SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		kiteableWindDetectedSerde.configure(serdeConfig, false);
//		return kiteableWindDetectedSerde;
//	}
//	
//	public static SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde(Properties envProps) {
//		final SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		windHasFallenOffSerde.configure(serdeConfig, false);
//		return windHasFallenOffSerde;
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
//	public static SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		kiteableWindDirectionDetectedSerde.configure(serdeConfig, false);
//		return kiteableWindDirectionDetectedSerde;
//	}
//	
//	public static SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetectedSerde(Properties envProps) {
//		final SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected = new SpecificAvroSerde<>();
//		Map<String, String> serdeConfig = new HashMap<>();
//		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
//		unkiteableWindDirectionDetected.configure(serdeConfig, false);
//		return unkiteableWindDirectionDetected;
//	}	
	
	protected static StreamsBuilder buildTopology(
			String windSpeedTopic,
			String waveHeightTopic,
			String windDirectionTopic,
			String outtopic,
			GenericAvroSerde recordSerde,
			SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde,
			SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetectedSerde,
			Properties streamProperties,
			StreamsBuilder builder
	) {
		
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
		
		var kiteableWindSpeedStream = builder.stream(windSpeedTopic, Consumed.with(Serdes.String(), recordSerde));
		var kiteableWaveHeightStream = builder.stream(waveHeightTopic, Consumed.with(Serdes.String(), recordSerde));
		var kiteableWindDirectionStream = builder.stream(windDirectionTopic, Consumed.with(Serdes.String(), recordSerde));

		String location = null;	// Will be chosen based on the sensors
		
		// I'm stuck here - Is it because I call the same processor over and over
		kiteableWindSpeedStream
			.merge(kiteableWaveHeightStream)
			.merge(kiteableWindDirectionStream)
//			.peek((k,v) -> System.out.print(String.format("Before processing -> key: %s, value: %s%n", k, v.getSchema().getName())))
			.process(() -> new KiteableWeatherReconProcessor(kvStoreNameKiteable, kvStoreNameUnkiteable, getLocation("NP7WC3")), kvStoreNameKiteable, kvStoreNameUnkiteable)
			.split()
			.branch((k,v) -> v.getSchema().getName().toString().startsWith("Kiteable"),
					Branched.withConsumer(s -> s
							.mapValues(v -> new KiteableWeatherDetected(String.format("%s%d",v.get("DataID").toString(), (long) v.get("Tijdstip")), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
							.peek((k,v) -> System.out.print(String.format("Kiteable branch -> key: %s, value: %s%n", k, v)))
							.to(outtopic, Produced.with(Serdes.String(), kiteableWeatherDetectedSerde))
							))
			.branch((k,v) -> v.getSchema().getName().startsWith("NoKiteable"),
					Branched.withConsumer(s -> s
							.mapValues(v -> new NoKiteableWeatherDetected(String.format("%s%d",v.get("DataID").toString(), (long) v.get("Tijdstip")), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
							.peek((k,v) -> System.out.print(String.format("Unkiteable branch -> key: %s, value: %s%n", k, v)))
							.to(outtopic, Produced.with(Serdes.String(), noKiteableWeatherDetectedSerde))
							));
//			.defaultBranch(Branched.withConsumer(s -> s
//					.mapValues(v -> new NoKiteableWeatherDetected(v.get("DataID").toString(), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
//					.peek((k,v) -> System.out.print(String.format("Unkiteable branch -> key: %s, value: %s%n", k, v)))
//					.to(outtopic, Produced.with(Serdes.String(), noKiteableWeatherDetectedSerde))
//					));
		
		return builder;
	}
	
	private static String getLocation(String sensorID) {
		
		String sensorLocation = sensorID.substring(0, Math.min(sensorID.length(), 3));
		String sensorType = sensorID.substring(sensorID.length() - 3);
		
//		switch(sensorLocation) {
//		case "NPB":
//			return "Nieuwpoort";
//		case "NP7":
//			return "Nieuwpoort";
//		default:
//			return "No location found";
//		}
		
		return "Nieuwpoort";
	}
}
