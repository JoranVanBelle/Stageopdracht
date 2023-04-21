package com.stage.adapter.mvb.integration;

import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import com.stage.KiteableWeatherDetected;
import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.consumers.KiteableWeatherConsumer;
import com.stage.adapter.mvb.infrastructure.EmailInfrastructure;
import com.stage.adapter.mvb.streams.KiteableWaveStream;
import com.stage.adapter.mvb.streams.KiteableWeatherStream;
import com.stage.adapter.mvb.streams.KiteableWindStream;
import com.stage.adapter.mvb.streams.KiteableWinddirectionStream;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.*;
import io.restassured.RestAssured;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
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
import org.junit.After;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.stage.adapter.mvb.Application;
import com.stage.adapter.mvb.extension.KafkaTestcontainer;
import com.stage.adapter.mvb.extension.MailhogTestcontainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
@ExtendWith(KafkaTestcontainer.class)
public class ApplicationIntegrationTest {

	private static KafkaContainer kafka;
	private GreenMail greenMail;
	private static PostgreSQLContainer postgreSQLContainer;

	private static final String schema_registry = "mock://test";
	private static final String topicName = "Meetnet.meting.raw";

	@BeforeAll
	public static void beforeAll() {
		kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

		postgreSQLContainer = new PostgreSQLContainer("postgres:11.1")
				.withDatabaseName("Stageopdracht")
				.withUsername("user")
				.withPassword("amdmin");

		postgreSQLContainer.withInitScript("populateDatabase.sql");

		kafka.start();
		postgreSQLContainer.start();

		prepareTest();
	}

	@AfterAll
	public static void afterAll() {
		kafka.close();
		postgreSQLContainer.close();
	}

	@BeforeEach
	public void beforeEach() {
		greenMail = new GreenMail(ServerSetupTest.SMTP);
		greenMail.start();
	}

	@AfterEach
	public void afterEach() {
		greenMail.stop();
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
				"localhost",
				greenMail.getSmtp().getPort(),
				"joran.vanbelle2@student.hogent.be",
				"e&FK@G82a$SE%8^rke77"
		);

		KiteableWeatherConsumer consumer = new KiteableWeatherConsumer(
				Application.getProperties(),
				postgreSQLContainer.getJdbcUrl(),
				postgreSQLContainer.getUsername(),
				postgreSQLContainer.getPassword(),
				"localhost",
				greenMail.getSmtp().getPort()
		);

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

		MimeMessage[] receivedMessages = greenMail.getReceivedMessages();
		assertEquals(1, receivedMessages.length);

		KiteableWeatherDetected weather = new KiteableWeatherDetected();
		weather.setDataID("NieuwpoortKiteable1");
		weather.setLocatie("Nieuwpoort");
		weather.setWindsnelheid("8.00");
		weather.setEenheidWindsnelheid("m/s");
		weather.setGolfhoogte("151.00");
		weather.setEenheidGolfhoogte("cm");
		weather.setWindrichting("20.00");
		weather.setEenheidWindrichting("deg");

		MimeMessage receivedMessage = receivedMessages[0];
		assertEquals("Kiteable weather detected at Nieuwpoort", subjectFromMessage(receivedMessage));
		assertEquals(EmailInfrastructure.getText(weather), emailTextFrom(receivedMessage));

	}

	private static void prepareTest() {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry);
		Producer<String, RawDataMeasured> producer = new KafkaProducer<>(props);

		RawDataMeasured rawDataMeasured1 = new RawDataMeasured("NPBGHA", "Nieuwpoort", "151.00", "cm", 1L);
		RawDataMeasured rawDataMeasured2 = new RawDataMeasured("NP7WC3", "Nieuwpoort", "20.00", "deg", 1L);
		RawDataMeasured rawDataMeasured3 = new RawDataMeasured("NP7WVC", "Nieuwpoort", "8.00", "m/s", 1L);

		producer.send(new ProducerRecord<>(topicName, rawDataMeasured1.getSensorID(), rawDataMeasured1));
		producer.send(new ProducerRecord<>(topicName, rawDataMeasured2.getSensorID(), rawDataMeasured2));
		producer.send(new ProducerRecord<>(topicName, rawDataMeasured3.getSensorID(), rawDataMeasured3));

		producer.close();
	}

	private static String subjectFromMessage(MimeMessage receivedMessage) throws MessagingException, MessagingException {
		return receivedMessage.getSubject();
	}

	private static String emailTextFrom(MimeMessage receivedMessage) throws IOException, MessagingException, IOException {
		return ((MimeMultipart) receivedMessage.getContent())
				.getBodyPart(0)
				.getContent()
				.toString();
	}

}