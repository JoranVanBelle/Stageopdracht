package com.stage.adapter.mvb.streams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KiteableWaveDetected;
import com.stage.KiteableWeatherDetected;
import com.stage.KiteableWindDetected;
import com.stage.KiteableWindDirectionDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.UnkiteableWaveDetected;
import com.stage.UnkiteableWindDetected;
import com.stage.UnkiteableWindDirectionDetected;
import com.stage.adapter.mvb.helpers.GracefulShutdown;
import com.stage.adapter.mvb.processors.KiteableWeatherReconProcessor;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWeatherStream extends Thread {

	private static final String WINDSPEEDTOPIC = "Meetnet.meting.wind.kiteable";
	private static final String WAVETOPIC = "Meetnet.meting.wave.kiteable";
	private static final String WINDDIRECTIONTOPIC = "Meetnet.meting.wind.direction.kiteable";
	private static final String OUTTOPIC = "Meetnet.meting.kiteable";
	
	
	private static final Logger logger = LogManager.getLogger(KiteableWeatherStream.class);
	
	private static final String kvStoreNameKiteable = "kiteableWeatherStream";
	private static final String kvStoreNameUnkiteable = "unkiteableWeatherStream";
	
	// Have to look for a better way than hard-coded
	private static final String kiteableWindSpeedSchemaName = "KiteableWindDetected";
	private static final String kiteableWaveSchemaName = "KiteableWaveDetected";
	private static final String kiteableWindDirectionSchemaName = "KiteableWindDirectionDetected";
	private static final String windHasFallenOffSchemaName = "WindHasFallenOff";
	private static final String unkiteableWaveSchemaName = "UnkiteableWaveDetected";
	private static final String unkiteableWindDirectionSchemaName = "UnkiteableWindDirectionDetected";
	
	private static final String[] sensoren = {"A2BGHA", "WDLGHA", "RA2GHA", "OSNGHA", "NPBGHA", "SWIGHA",
										"MP0WC3", "MP7WC3", "NP7WC3", "MP0WVC", "MP7WVC", "NP7WVC", "A2BRHF", "RA2RHF", "OSNRHF"};
	
	@Override
	public void run() {
		Properties props = streamsConfig();
		
		Topology topology = buildTopology(
				WINDSPEEDTOPIC, 
				WAVETOPIC, 
				WINDDIRECTIONTOPIC, 
				OUTTOPIC, 
				genericRecordSerde(props), 
				kiteableWeatherDetectedSerde(props), 
				noKiteableWeatherDetectedSerde(props), 
				windHasFallenOffSchemaName, 
				unkiteableWaveSchemaName, 
				unkiteableWindDirectionSchemaName, 
				kiteableWindSpeedSchemaName, 
				kiteableWaveSchemaName, 
				kiteableWindDirectionSchemaName, 
				props
		);
		
		KafkaStreams streams = new KafkaStreams(topology, props);
		
		streams.start();
		logger.info("ℹ️ KiteableWeatherStream started");
		
		GracefulShutdown.gracefulShutdown(streams);
				
	}
	
	private static Properties streamsConfig() {
		Properties settings = new Properties();

		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, Optional.ofNullable(System.getenv("APP_ID")).orElseThrow(() -> new IllegalArgumentException("APP_ID is required")));
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS")).orElseThrow(() -> new IllegalArgumentException("BOOTSTRAP_SERVERS is required")));
		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Optional.ofNullable(System.getenv("SCHEMA_REGISTRY_URL")).orElseThrow(() -> new IllegalArgumentException("SCHEMA_REGISTRY_URL is required")));

		return settings;
	}
	
	public static GenericAvroSerde genericRecordSerde(Properties envProps) {
		GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		genericAvroSerde.configure(serdeConfig, false);
		return genericAvroSerde;
	}
	
	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWeatherDetected.configure(serdeConfig, false);
		return kiteableWeatherDetected;
	}
	
	public static SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		noKiteableWeatherDetected.configure(serdeConfig, false);
		return noKiteableWeatherDetected;
	}
	
	public static SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWindDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDetectedSerde;
	}
	
	public static SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde(Properties envProps) {
		final SpecificAvroSerde<UnkiteableWindDetected> windHasFallenOffSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		windHasFallenOffSerde.configure(serdeConfig, false);
		return windHasFallenOffSerde;
	}
	
	public static SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<KiteableWaveDetected> kiteableWaveDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWaveDetectedSerde.configure(serdeConfig, false);
		return kiteableWaveDetectedSerde;
	}
	
	public static SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<UnkiteableWaveDetected> unkiteableWaveDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		unkiteableWaveDetected.configure(serdeConfig, false);
		return unkiteableWaveDetected;
	}
	
	public static SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWindDirectionDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDirectionDetectedSerde;
	}
	
	public static SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
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
			String windHasFallenOffSchema,
			String unkiteableWaveSchema,
			String unkiteableWindDirectionSchema,
			String kiteableWindSpeedSchema,
			String kiteableWaveSchema,
			String kiteableWindDirectionSchema,
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
							.mapValues(v -> new KiteableWeatherDetected(v.get("DataID").toString(), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
							.peek((k,v) -> System.out.print(String.format("Kiteable branch -> key: %s, value: %s%n", k, v)))
							.to(outtopic, Produced.with(Serdes.String(), kiteableWeatherDetectedSerde))
							))
			.branch((k,v) -> v.getSchema().getName().startsWith("NoKiteable"),
					Branched.withConsumer(s -> s
							.mapValues(v -> new NoKiteableWeatherDetected(v.get("DataID").toString(), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
							.peek((k,v) -> System.out.print(String.format("Unkiteable branch -> key: %s, value: %s%n", k, v)))
							.to(outtopic, Produced.with(Serdes.String(), noKiteableWeatherDetectedSerde))
							));
//			.defaultBranch(Branched.withConsumer(s -> s
//					.mapValues(v -> new NoKiteableWeatherDetected(v.get("DataID").toString(), v.get("Locatie").toString(), v.get("Windsnelheid").toString(), v.get("EenheidWindsnelheid").toString(), v.get("Golfhoogte").toString(), v.get("EenheidGolfhoogte").toString(), v.get("Windrichting").toString(), v.get("EenheidWindrichting").toString(), (long) v.get("Tijdstip")))
//					.peek((k,v) -> System.out.print(String.format("Unkiteable branch -> key: %s, value: %s%n", k, v)))
//					.to(outtopic, Produced.with(Serdes.String(), noKiteableWeatherDetectedSerde))
//					));
		
		return builder.build(streamProperties);
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
