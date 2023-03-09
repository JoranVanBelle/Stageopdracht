package com.stage.adapter.mvb.streams;

import java.time.Duration;
import java.util.*;

import com.stage.KiteableWindDetected;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.suppress.NamedSuppressed;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.stage.KiteableWindDetected;
import com.stage.RawDataMeasured;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

// https://stackoverflow.com/questions/58745670/kafka-compare-consecutive-values-for-a-key

public class KiteableWindStream extends Thread {

	private static final String INTOPIC = "Meetnet.meting.raw";
	private static final String WINDTOPIC = "Meetnet.meting.wind.kitable";
	private final List<String> SENSOREN = new ArrayList<String>(Arrays.asList(new String[] {"NP7WC3"}));

	private static final Logger logger = LogManager.getLogger(KiteableWindStream.class);

	@Override
	public void run() {
		Properties props = streamsConfig();

		Topology topo = buildTopology(SENSOREN, 7.71, INTOPIC, WINDTOPIC, rawDataMeasuredSerde(props), kiteableWindDetectedSerde(props), props);
		KafkaStreams streams = new KafkaStreams(topo, props);
		streams.start();
		logger.info("ℹ️ KiteableWindStream started");

	}

	public static SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde(Properties envProps) {
		final SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		rawDataMeasuredSerde.configure(serdeConfig, false);
		return rawDataMeasuredSerde;
	}

	public static SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde(Properties envProps) {
		final SpecificAvroSerde<KiteableWindDetected> kiteableWindDetectedSerde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWindDetectedSerde.configure(serdeConfig, false);
		return kiteableWindDetectedSerde;
	}

//
//	private static Properties getProperties() {
//
//		Properties props = new Properties();
//
//		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
//		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
//		props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
//		props.put(StreamsConfig.APPLICATION_ID_CONFIG, KiteableWindStream.class.toString());
//
//		props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
//		props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "snappy");
//		props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 3);
//		props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 500);
//        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
//
//        return props;
//	}

	private static Properties streamsConfig() {
		Properties settings = new Properties();
		// Set a few key parameters
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, Optional.ofNullable(System.getenv("APP_ID")).orElseThrow(() -> new IllegalArgumentException("APP_ID is required")));
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS")).orElseThrow(() -> new IllegalArgumentException("BOOTSTRAP_SERVERS is required")));
		settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Optional.ofNullable(System.getenv("SCHEMA_REGISTRY_URL")).orElseThrow(() -> new IllegalArgumentException("SCHEMA_REGISTRY_URL is required")));
		//todo add extra configuration

		// Any further settings
//        settings.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, Optional.ofNullable(System.getenv("QS_SECURITY_PROTOCOL")).orElse("SASL_SSL"));
//        settings.put("sasl.jaas.config", Optional.ofNullable(System.getenv("QS_JAAS_CONFIG")).orElseThrow(() -> new IllegalArgumentException("QS_JAAS_CONFIG is required")));
//        settings.put("ssl.endpoint.identification.algorithm", Optional.ofNullable(System.getenv("QS_ENDPOINT_ID_ALG")).orElse("https"));
//        settings.put("sasl.mechanism", Optional.ofNullable(System.getenv("QS_SASL_MECH")).orElse("PLAIN"));
//        settings.put("replication.factor", Optional.ofNullable(System.getenv("QS_REPLICATION")).orElse("3"));
//        settings.put("auto.offset.reset", Optional.ofNullable(System.getenv("QS_AUTO_OFFSET_RESET")).orElse("earliest"));

		return settings;
	}

	protected static Topology buildTopology(Collection<String> inScopeSensors,
										  double windspeedTreshhold,
										  String rawDataTopic,
										  String kiteableWindTopic,
										  SpecificAvroSerde<RawDataMeasured> rawDataMeasuredSerde,
										  SpecificAvroSerde<KiteableWindDetected> kitableWindDetectedSerde,
										  Properties streamProperties
	){
		StreamsBuilder builder = new StreamsBuilder();

		builder.addStateStore(
				Stores.keyValueStoreBuilder(
						Stores.persistentKeyValueStore("most-recent-event"),
						Serdes.String(),
						rawDataMeasuredSerde)
		);

		builder.stream(rawDataTopic, Consumed.with(Serdes.String(), rawDataMeasuredSerde))
				.filter(onlyInScopeSensors(inScopeSensors))
				.groupByKey() // We willen detecties doen per sensor...
				.reduce((previousValue, currentValue) -> measurementsThatCrossTheTreshhold(windspeedTreshhold, previousValue, currentValue))
				.mapValues(KiteableWindStream::transformToKiteableWindDetected)
//				.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(5), Suppressed.BufferConfig.unbounded()))
				.toStream()
				.peek((k, v) -> {logger.info(String.format("ℹ️ Sensor: %s: %s", k, v));})
				.to(kiteableWindTopic, Produced.with(Serdes.String(), kitableWindDetectedSerde));

		return builder.build(streamProperties);
	}

	private static KiteableWindDetected transformToKiteableWindDetected(RawDataMeasured v) {
		return new KiteableWindDetected(v.getSensorID(), v.getLocatie(), v.getTijdstip());
	}

	private static RawDataMeasured measurementsThatCrossTheTreshhold(double windspeedTreshhold, RawDataMeasured previousValue, RawDataMeasured currentValue) {
		if(previousValue == null) {
			return currentValue;
		}

		var previousParsedWindspeed = Double.parseDouble(previousValue.getWaarde());
		var currentParsedWindspeed = Double.parseDouble(currentValue.getWaarde());

		if(currentParsedWindspeed > windspeedTreshhold){
			if (previousParsedWindspeed <= windspeedTreshhold){
				return currentValue;
			} else {
				return previousValue;
			}

		}
//		else {
//						// Not kiteable anymore... We could publish an event to lag that the wind is no longer kitable.
//					}
		return previousValue;
	}

	private static Predicate<String, RawDataMeasured> onlyInScopeSensors(Collection<String> inScopeSensors) {
		return (key_maybe, v) -> Optional.ofNullable(key_maybe).map(inScopeSensors::contains).orElse(false);
	}




}
