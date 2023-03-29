package com.stage.adapter.mvb.consumers;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGPoolingDataSource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext.Empty;
import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.adapter.mvb.database.AbstractContainerDatabaseTest;
import com.stage.adapter.mvb.database.PostgreSQLTestImages;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

//@ExtendWith(MockitoExtension.class)
public class KiteableWeatherConsumerTest extends AbstractContainerDatabaseTest {
	 private static final Random random = new Random();
	
	private static final String KITEABLE_WEATHER_TOPIC = "kiteable";
	
	private static MockConsumer<String, GenericRecord> consumer;
	private static KiteableWeatherConsumer kiteableWeatherConsumer;
	public static PostgreSQLContainer<?> postgres;
	public static GenericContainer<?> mailhog;
	
	private static final Integer PORT_DB = 5432;
    private static final Integer PORT_SMTP = 1025;
    private static final Integer PORT_HTTP = 8025;
	
    private static Integer dbPort;
    private static Integer smtpPort;
    private static String smtpHost;
    private static Integer httpPort;
    
    
    
	@BeforeAll
	static void setupDatabase() {
		postgres = new PostgreSQLContainer<>(PostgreSQLTestImages.POSTGRES_TEST_IMAGE)
				.withInitScript("populateDatabase.sql")
				.withCreateContainerCmdModifier( cmd -> cmd.withName( "postgreSQL-" + (random.nextInt() & Integer.MAX_VALUE)));
		postgres.start();
		
		mailhog = new GenericContainer<>("mailhog/mailhog")
			    .withExposedPorts(PORT_SMTP, PORT_HTTP)
				.withCreateContainerCmdModifier( cmd -> cmd.withName( "mailhog-" + (random.nextInt() & Integer.MAX_VALUE)));
		mailhog.start();
	}
	
	@BeforeEach
	void setup() throws SQLException {
		prepareForANewTest();
	}

	@AfterEach
	void teardown() throws SQLException {
		consumer.close();
		kiteableWeatherConsumer.stopConsumer();
		cleanUpMess();
	} 

	@AfterAll
	static void shutdown() {
		postgres.close();
		mailhog.close();
	}
	

	
    @Test
    public void testSimple() throws SQLException {
    	
	    ResultSet resultSet = performQuery(postgres, "SELECT 1");
	    
	    int resultSetInt = resultSet.getInt(1);
	    assertThat(resultSetInt).as("A basic SELECT query succeeds").isEqualTo(1);
	    assertHasCorrectExposedAndLivenessCheckPorts(postgres);
    }
    
	
	@Test
	public void testContentOfRecordInDatabase() throws SQLException {

		consumer.schedulePollTask(() -> {
			consumer.rebalance(Collections.singletonList(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 0L, "NieuwpoortKiteable", new KiteableWeatherDetected("NieuwpoortKiteable", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 1L)));
		});
		
		consumer.schedulePollTask(() -> kiteableWeatherConsumer.stopConsumer());
		
	    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
	    beginningOffsets.put(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0), 0L);
	    consumer.updateBeginningOffsets(beginningOffsets);
		
		kiteableWeatherConsumer.writeToDatabase(getConnection(postgres));
		
		ResultSet resultSet = performQuery(postgres, "SELECT * FROM Kiten");
		
		Assertions.assertEquals("NieuwpoortKiteable", resultSet.getString(1));
		Assertions.assertEquals("Nieuwpoort", resultSet.getString(2));
		Assertions.assertEquals("9.00", resultSet.getString(3));
		Assertions.assertEquals("m/s", resultSet.getString(4));
		Assertions.assertEquals("151.00", resultSet.getString(5));
		Assertions.assertEquals("cm", resultSet.getString(6));
		Assertions.assertEquals("10.00", resultSet.getString(7));
		Assertions.assertEquals("deg", resultSet.getString(8));
		Assertions.assertEquals(1, resultSet.getLong(9));
	}
	
	@Test
	public void testAmountOfRecordsInDatabase() throws SQLException {
		consumer.schedulePollTask(() -> {
			consumer.rebalance(Collections.singletonList(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0)));
			
		    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
		    beginningOffsets.put(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0), 0L);
		    consumer.updateBeginningOffsets(beginningOffsets);
			
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 0L, "NieuwpoortKiteable1", new KiteableWeatherDetected("NieuwpoortKiteable1", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 1L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 1L, "NieuwpoortUnkiteable2", new NoKiteableWeatherDetected("NieuwpoortUnkiteable2", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 2L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 2L, "NieuwpoortUnkiteable3", new NoKiteableWeatherDetected("NieuwpoortUnkiteable3", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 3L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 3L, "NieuwpoortKiteable4", new KiteableWeatherDetected("NieuwpoortKiteable4", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 4L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 4L, "NieuwpoortKiteable5", new KiteableWeatherDetected("NieuwpoortKiteable5", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 5L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 5L, "NieuwpoortUnkiteable6", new NoKiteableWeatherDetected("NieuwpoortUnkiteable6", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 6L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 6L, "NieuwpoortKiteable7", new KiteableWeatherDetected("NieuwpoortKiteable7", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 7L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 7L, "NieuwpoortUnkiteable8", new NoKiteableWeatherDetected("NieuwpoortUnkiteable8", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 8L)));
		});
		
	    Map<TopicPartition, Long> endingOffsets = new HashMap<>();
	    endingOffsets.put(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0), 7L);
	    consumer.updateEndOffsets(endingOffsets);
		
		consumer.schedulePollTask(() -> kiteableWeatherConsumer.stopConsumer());
	    
		kiteableWeatherConsumer.writeToDatabase(getConnection(postgres));
		
		ResultSet resultSet = performQuery(postgres, "SELECT COUNT(DataID) FROM Kiten");
		
		Assertions.assertEquals(8, resultSet.getInt(1));
	}
	
	@Test
	void testEmailSent() throws SQLException {
		

		
		consumer.schedulePollTask(() -> {
			consumer.rebalance(Collections.singletonList(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 0L, "NieuwpoortKiteable", new KiteableWeatherDetected("NieuwpoortKiteable", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 1L)));
		});
		
		consumer.schedulePollTask(() -> kiteableWeatherConsumer.stopConsumer());
		
	    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
	    beginningOffsets.put(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0), 0L);
	    consumer.updateBeginningOffsets(beginningOffsets);
		
	    
	    given().when().get("/messages").then().body("", Matchers.hasSize(0));
	    
		kiteableWeatherConsumer.writeToDatabase(getConnection(postgres));
		System.out.println(given().get("/messages").asPrettyString());
		given().when().get("/messages").then().body("", Matchers.not(Matchers.hasSize(0)));	
		
		given().delete("/messages");
		given().when().get("/messages").then().body("", Matchers.hasSize(0));
	}
	
	@Test
	void testFourEmailsSent() throws SQLException {
		
		consumer.schedulePollTask(() -> {
			consumer.rebalance(Collections.singletonList(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0)));
			
		    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
		    beginningOffsets.put(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0), 0L);
		    consumer.updateBeginningOffsets(beginningOffsets);
			
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 0L, "NieuwpoortKiteable1", new KiteableWeatherDetected("NieuwpoortKiteable1", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 1L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 1L, "NieuwpoortUnkiteable2", new NoKiteableWeatherDetected("NieuwpoortUnkiteable2", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 2L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 2L, "NieuwpoortUnkiteable3", new NoKiteableWeatherDetected("NieuwpoortUnkiteable3", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 3L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 3L, "NieuwpoortKiteable4", new KiteableWeatherDetected("NieuwpoortKiteable4", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 4L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 4L, "NieuwpoortKiteable5", new KiteableWeatherDetected("NieuwpoortKiteable5", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 5L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 5L, "NieuwpoortUnkiteable6", new NoKiteableWeatherDetected("NieuwpoortUnkiteable6", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 6L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 6L, "NieuwpoortKiteable7", new KiteableWeatherDetected("NieuwpoortKiteable7", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 7L)));
			consumer.addRecord(new ConsumerRecord<String, GenericRecord>(KITEABLE_WEATHER_TOPIC, 0, 7L, "NieuwpoortUnkiteable8", new NoKiteableWeatherDetected("NieuwpoortUnkiteable8", "Nieuwpoort", "9.00", "m/s", "151.00", "cm", "10.00", "deg", 8L)));
		});
		
	    Map<TopicPartition, Long> endingOffsets = new HashMap<>();
	    endingOffsets.put(new TopicPartition(KITEABLE_WEATHER_TOPIC, 0), 7L);
	    consumer.updateEndOffsets(endingOffsets);
		
		consumer.schedulePollTask(() -> kiteableWeatherConsumer.stopConsumer());
		
		kiteableWeatherConsumer.writeToDatabase(getConnection(postgres));

		System.out.println(given().get("/messages").asPrettyString());
		
		given().when().get("/messages").then().body("", Matchers.hasSize(4));	
	}
	
	private void assertHasCorrectExposedAndLivenessCheckPorts(PostgreSQLContainer<?> postgres) {
		assertThat(postgres.getExposedPorts()).containsExactly(PostgreSQLContainer.POSTGRESQL_PORT);
		assertThat(postgres.getLivenessCheckPortNumbers())
		.containsExactly(postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
	}
		
	
	private static JavaMailSender getEmailConfigTest() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
		
		mailSender.setHost(smtpHost);
		mailSender.setPort(smtpPort);
		
		Properties props = mailSender.getJavaMailProperties();
	    props.put("mail.transport.protocol", "smtp");
	    props.put("mail.smtp.auth", "false");
	    props.put("mail.smtp.starttls.enable", "false");
	    props.put("mail.debug", "false");
		
		return mailSender;
	}
	
	private static PGPoolingDataSource getDatabaseConfigTest() {
		PGPoolingDataSource source = new PGPoolingDataSource();
						
		source.setPortNumber(dbPort);
		
		return source;
	}
	
	private static void prepareForANewTest() throws SQLException {
		dbPort = postgres.getMappedPort(PORT_DB);
		
        smtpPort = mailhog.getMappedPort(PORT_SMTP);
        smtpHost = mailhog.getHost();
        httpPort = mailhog.getMappedPort(PORT_HTTP);

        RestAssured.baseURI = "http://" + smtpHost;
        RestAssured.port = httpPort;
        RestAssured.basePath = "/api/v1";
		
		consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
		kiteableWeatherConsumer = new KiteableWeatherConsumer(consumer, getDatabaseConfigTest(), getEmailConfigTest());

		performQuery(postgres, "INSERT INTO Users(Email, Username) VALUES ('joran.vanbelle2@student.hogent.be', 'Joran')");
		performQuery(postgres, "INSERT INTO Nieuwpoort(Email) VALUES ('joran.vanbelle2@student.hogent.be')");		
	}
	
	private static void cleanUpMess() throws SQLException {
		performQuery(postgres, "DELETE FROM Kiten");
		performQuery(postgres, "DELETE FROM Nieuwpoort");
		performQuery(postgres, " DELETE FROM Users");
		clearMailHog(httpPort);
	}
	
	private static void clearMailHog(int httpPort) {
		URI uri;

		try {
//			String url = String.format("%s:%d%s/messages", RestAssured.baseURI, httpPort, RestAssured.basePath);
//			uri = new URI(url);
////			System.err.println(url);
//			HttpClient client = HttpClient.newHttpClient();
//			HttpRequest request = HttpRequest.newBuilder()
//				    .uri(uri)
//				    .DELETE()
//				    .build();
//			
//			client.send(request, HttpResponse.BodyHandlers.ofString());
			
//					  RestAssured.baseURI = baseUrl;

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
