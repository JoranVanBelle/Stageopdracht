package com.stage.adapter.mvb.streams;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KiteableWaveDetected;
import com.stage.KiteableWeatherDetected;
import com.stage.KiteableWindDetected;
import com.stage.KiteableWindDirectionDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.RawDataMeasured;
import com.stage.UnkiteableWaveDetected;
import com.stage.UnkiteableWindDetected;
import com.stage.UnkiteableWindDirectionDetected;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class MergedWeatherStream extends Thread {

	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WINDSPEEDTOPIC = "Meetnet.meting.wind.direction.kitable";
	private static final String WAVETOPIC = "Meetnet.meting.wave.kitable";
	private static final String WINDDIRECTIONTOPIC = "Meetnet.meting.wind.direction.kiteable";
	private static final String OUTTOPIC = "Meetnet.meting.kiteable";
	
	private static final Logger logger = LogManager.getLogger(MergedWeatherStream.class);
	
	private static final double waveheightThreshold = 150;
	private static final double windspeedThreshold = 7.717;
	
	private final List<String> waveheightSensors = new ArrayList<String>(Arrays.asList(new String[] {"NPBGHA"}));
	private final List<String> windspeedSensors = new ArrayList<String>(Arrays.asList(new String[] {"NP7WC3"}));
	private final Map<String, double[]> winddirectionThreshold  = new HashMap<>() {{
	    put("NP7WRS", new double[] {10.00, 230.00});
	}};
	
	private final String app_id;
	private final String bootstrap_servers;
	private final String schema_registry;
	
	public MergedWeatherStream(String app_id, String bootstrap_servers, String schema_registry) {
		this.app_id = app_id;
		this.bootstrap_servers = bootstrap_servers;
		this.schema_registry = schema_registry;
	}
	
	@Override
	public void run() {
		
		StreamsBuilder builder = new StreamsBuilder();
		
//		KiteableWaveStream waveStream = new KiteableWaveStream();
//		KiteableWindStream windspeedStream = new KiteableWindStream();
//		KiteableWinddirectionStream winddirectionStream = new KiteableWinddirectionStream();
//		KiteableWeatherStream weatherStream = new KiteableWeatherStream();
		
		StreamsBuilder waveStreamsBuilder = KiteableWaveStream.buildTopology(
												waveheightSensors, 
												waveheightThreshold, 
												INTOPIC, WAVETOPIC, 
												rawDataMeasuredSerde(schema_registry), 
												kiteableWaveDetectedSerde(schema_registry), 
												unkiteableWaveDetectedSerde(schema_registry), 
												streamsConfig(app_id, bootstrap_servers, schema_registry), 
												builder);
		
		StreamsBuilder windspeedStreamsBuilder = KiteableWindStream.buildTopology(
													windspeedSensors, 
													windspeedThreshold, 
													INTOPIC, WINDSPEEDTOPIC, 
													rawDataMeasuredSerde(schema_registry), 
													kiteableWindDetectedSerde(schema_registry), 
													windHasFallenOffSerde(schema_registry), 
													streamsConfig(app_id, bootstrap_servers, schema_registry), 
													waveStreamsBuilder);
		
		StreamsBuilder winddirectionStreamsBuilder = KiteableWinddirectionStream.buildTopology(
														winddirectionThreshold, 
														INTOPIC, WINDDIRECTIONTOPIC, 
														rawDataMeasuredSerde(schema_registry), 
														kiteableWinddirectionDetectedSerde(schema_registry), 
														unkiteableWinddirectionDetected(schema_registry), 
														streamsConfig(app_id, bootstrap_servers, schema_registry), 
														windspeedStreamsBuilder);
		
		StreamsBuilder weatherStreamsBuilder = KiteableWeatherStream.buildTopology(
													WINDSPEEDTOPIC, WAVETOPIC, 
													WINDDIRECTIONTOPIC, OUTTOPIC, 
													genericRecordSerde(schema_registry), 
													kiteableWeatherDetectedSerde(schema_registry), 
													noKiteableWeatherDetectedSerde(schema_registry), 
													streamsConfig(app_id, bootstrap_servers, schema_registry), 
													winddirectionStreamsBuilder);
		
		Topology topo = weatherStreamsBuilder.build();
		
		KafkaStreams streams = new KafkaStreams(topo, streamsConfig(app_id, bootstrap_servers, schema_registry));
		
		logger.info("ℹ️ Kafka streams started");
		
		streams.start();
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
	
	public static SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWinddirectionDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWindDirectionDetected> kiteableWindDirectionDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWindDirectionDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDirectionDetectedSerde;
	}
	
	public static SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWinddirectionDetected(String schema_registry) {
		final SpecificAvroSerde<UnkiteableWindDirectionDetected> unkiteableWindDirectionDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		unkiteableWindDirectionDetected.configure(serdeConfig, false);
		return unkiteableWindDirectionDetected;
	}
	
	public static GenericAvroSerde genericRecordSerde(String schema_registry) {
		GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		genericAvroSerde.configure(serdeConfig, false);
		return genericAvroSerde;
	}
	
	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		kiteableWeatherDetected.configure(serdeConfig, false);
		return kiteableWeatherDetected;
	}
	
	public static SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetectedSerde(String schema_registry) {
		final SpecificAvroSerde<NoKiteableWeatherDetected> noKiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		noKiteableWeatherDetected.configure(serdeConfig, false);
		return noKiteableWeatherDetected;
	}
	
	private static Properties streamsConfig(String app_id, String bootstrap_servers, String schema_registry) {
		
		Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, app_id);
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class); 
		
		return settings;
	}
}
