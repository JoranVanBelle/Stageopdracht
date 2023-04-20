package com.stage.adapter.mvb.integration;

import com.stage.KiteableWeatherDetected;
import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.consumers.KiteableWeatherConsumer;
import com.stage.adapter.mvb.streams.KiteableWaveStream;
import com.stage.adapter.mvb.streams.KiteableWeatherStream;
import com.stage.adapter.mvb.streams.KiteableWindStream;
import com.stage.adapter.mvb.streams.KiteableWinddirectionStream;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.*;
import io.restassured.RestAssured;
import jakarta.mail.internet.MimeMessage;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.checkerframework.checker.units.qual.K;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.adapter.mvb.Application;
import com.stage.adapter.mvb.extension.KafkaTestcontainer;
import com.stage.adapter.mvb.extension.MailhogTestcontainer;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.restassured.RestAssured.given;

@ExtendWith(MockitoExtension.class)
@ExtendWith(KafkaTestcontainer.class)
@ExtendWith(MailhogTestcontainer.class)
public class ApplicationIntegrationTest {

	private static KafkaContainer kafka;
	private static PostgreSQLContainer postgreSQLContainer;
	private static GenericContainer<?> mailhog;
	private static final Random random = new Random();

	private static final Integer PORT_SMTP = 1025;
	private static final Integer PORT_HTTP = 8025;

	private static final String schema_registry = "mock://test";
	private static final String topicName = "Meetnet.meting.raw";

	@BeforeAll
	public static void beforeAll() {
		kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

		mailhog = new GenericContainer<>("mailhog/mailhog")
				.withExposedPorts(PORT_SMTP, PORT_HTTP);

		postgreSQLContainer = new PostgreSQLContainer("postgres:11.1")
				.withDatabaseName("Stageopdracht")
				.withUsername("user")
				.withPassword("amdmin");

		postgreSQLContainer.withInitScript("populateDatabase.sql");

		kafka.start();
		postgreSQLContainer.start();
		mailhog.start();

		prepareTest();
	}

	@AfterAll
	public static void afterAll() {
		kafka.close();
		postgreSQLContainer.close();
		mailhog.close();
	}

	@Test
	public void emailIntegrationTest() throws Exception {

		Application.setup(
				"api_meetnetVlaamseBanken",
				postgreSQLContainer.getJdbcUrl(),
				postgreSQLContainer.getUsername(),
				postgreSQLContainer.getPassword(),
				"api_username",
				"api_password",
				"integration_test",
				kafka.getBootstrapServers(),
				schema_registry,
				mailhog.getHost(),
				mailhog.getMappedPort(PORT_HTTP)
		);

		KiteableWeatherConsumer consumer = new KiteableWeatherConsumer(
				Application.getProperties(),
				postgreSQLContainer.getJdbcUrl(),
				postgreSQLContainer.getUsername(),
				postgreSQLContainer.getPassword(),
				mailhog.getHost(),
				mailhog.getMappedPort(PORT_HTTP));

		KiteableWaveStream waveStream = new KiteableWaveStream("integration_test", kafka.getBootstrapServers(), schema_registry);
		KiteableWindStream windspeedStream = new KiteableWindStream("integration_test", kafka.getBootstrapServers(), schema_registry);
		KiteableWinddirectionStream winddirectionStream = new KiteableWinddirectionStream("integration_test", kafka.getBootstrapServers(), schema_registry);
		KiteableWeatherStream weatherStream = new KiteableWeatherStream("integration_test", kafka.getBootstrapServers(), schema_registry);

		Thread comsumerThread = new Thread(consumer);

		Thread waveStreamThread = new Thread(waveStream);
		Thread windspeedStreamThread = new Thread(windspeedStream);
		Thread winddirectionStreamThread = new Thread(winddirectionStream);
		Thread weatherStreamThread = new Thread(weatherStream);

		waveStreamThread.start();
		windspeedStreamThread.start();
		winddirectionStreamThread.start();

		Thread.sleep(5000);

		weatherStreamThread.start();
		comsumerThread.start();

		Thread.sleep(10000);

		System.out.println(given().get("/messages").asPrettyString());
		given().when().get("/messages").then().body("total", Matchers.hasSize(1));

	}

	private static void prepareTest() {

		int smtpPort;
		String smtpHost;

		smtpPort = mailhog.getMappedPort(PORT_SMTP);
		smtpHost = mailhog.getHost();
		Integer httpPort = mailhog.getMappedPort(PORT_HTTP);

		RestAssured.baseURI = "http://" + mailhog.getHost();
		RestAssured.port = httpPort;
		RestAssured.basePath = "/api/v2";

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		Producer<String, RawDataMeasured> producer = new KafkaProducer<>(props);

		RawDataMeasured rawDataMeasured1 = new RawDataMeasured("NPBGHA", "Nieuwpoort", "151", "cm", 1L);
		RawDataMeasured rawDataMeasured2 = new RawDataMeasured("NP7WC3", "Nieuwpoort", "20", "deg", 1L);
		RawDataMeasured rawDataMeasured3 = new RawDataMeasured("NP7WVC", "Nieuwpoort", "8", "m/s", 1L);

		producer.send(new ProducerRecord<>(topicName, rawDataMeasured1.getSensorID(), rawDataMeasured1));
		producer.send(new ProducerRecord<>(topicName, rawDataMeasured2.getSensorID(), rawDataMeasured2));
		producer.send(new ProducerRecord<>(topicName, rawDataMeasured3.getSensorID(), rawDataMeasured3));

		producer.close();
	}

}