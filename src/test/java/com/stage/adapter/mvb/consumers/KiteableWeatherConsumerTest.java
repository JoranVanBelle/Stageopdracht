package com.stage.adapter.mvb.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.adapter.mvb.database.AbstractContainerDatabaseTest;
import com.stage.adapter.mvb.database.PostgreSQLTestImages;

// Documentatie embedded PostgreSQL

public class KiteableWeatherConsumerTest extends AbstractContainerDatabaseTest {

	private static final String KITEABLE_WEATHER_TOPIC = "kiteable";
	
	private MockConsumer<String, GenericRecord> consumer;
	private KiteableWeatherConsumer kiteableWeatherConsumer;
	static PostgreSQLContainer<?> postgres;
	
	private static final String url = "jdbc:tc:postgresql:9.6.8:///postgrestestdv";

	@BeforeAll
	static void setupDatabase() {
		
		postgres = new PostgreSQLContainer<>(PostgreSQLTestImages.POSTGRES_TEST_IMAGE)
				.withInitScript("populateDatabase.sql");
		postgres.start();
	}
	
	@BeforeEach
	void setup() throws SQLException {
		consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
		kiteableWeatherConsumer = new KiteableWeatherConsumer(consumer);
		performQuery(postgres, "DELETE FROM Kiten");
	}

	@AfterEach
	void teardown() {
		consumer.close();
		kiteableWeatherConsumer.stopConsumer();
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
	
	private void assertHasCorrectExposedAndLivenessCheckPorts(PostgreSQLContainer<?> postgres) {
		assertThat(postgres.getExposedPorts()).containsExactly(PostgreSQLContainer.POSTGRESQL_PORT);
		assertThat(postgres.getLivenessCheckPortNumbers())
		.containsExactly(postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
	}
	

	
}
