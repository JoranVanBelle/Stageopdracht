package com.stage.adapter.mvb.consumers;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGPoolingDataSource;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;

import com.stage.KiteableWeatherDetected;
import com.stage.adapter.mvb.database.Database;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KiteableWeatherConsumer extends Thread {

	private final Properties props;
	private Database database;
	private Consumer<String, GenericRecord> consumer;
	private JavaMailSender emailSender;
	
	private static final String TOPIC = "Meetnet.meting.kiteable";
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private static final Logger logger = LogManager.getLogger(KiteableWeatherConsumer.class);
	
	public KiteableWeatherConsumer(Properties props) {
		this.props = props;
		this.consumer = new KafkaConsumer<>(this.props);
		database = new Database();
		emailSender = getMailingProps();
	}
	
	// Testpurposes
	public KiteableWeatherConsumer(MockConsumer<String, GenericRecord> consumer, PGPoolingDataSource mockSource, JavaMailSender emailSender) {
		this.props = null;
		this.consumer = consumer;
		database = new Database(mockSource);
		this.emailSender = emailSender;
	}
	
	@Override
	public void run() {
		Connection connection;
		try {
			connection = database.getSource().getConnection("user", "admin");
			writeToDatabase(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public void writeToDatabase(Connection connection) throws MailException, SQLException {	
		consumer.subscribe(Arrays.asList(TOPIC));
		
		logger.info("ℹ️ Consumer will start writing to the database");
		
		while(!closed.get()) {
			ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, GenericRecord> record : records) {
				GenericRecord value = record.value();
				if(schemaIsKnown(value)) {
					if(schemaIsKiteable(value)) {
						var kiteable = (KiteableWeatherDetected) SpecificData.get().deepCopy(value.getSchema(), value);
						sendEmail(kiteable, connection);
					}
					
					insertDataIntoDatabase(value, connection);
					
				} else {
					throw new IllegalArgumentException("Unknown schema in topic");
				}
			}
		}
	}
	
	private static boolean schemaIsKnown(GenericRecord value) {
		return value.getSchema().getName().equals("KiteableWeatherDetected") || value.getSchema().getName().equals("NoKiteableWeatherDetected");
	}
	
	private static boolean schemaIsKiteable(GenericRecord value) {
		return value.getSchema().getName().equals("KiteableWeatherDetected");
	}
	
	public void sendEmail(KiteableWeatherDetected content, Connection connection) throws MailException {
		
		String[] emailStrings = collectEmailAddresses(content.getLocatie(), connection);
	    
	    if(emailStrings != null) {
		    MimeMessagePreparator messagePreparator = mimeMessage -> {
		        MimeMessageHelper messageHelper = new MimeMessageHelper(mimeMessage, true);
		        messageHelper.setFrom("joran.vanbelle@live.be");
		        messageHelper.setReplyTo("joran.vanbelle@live.be");
		        messageHelper.setTo(emailStrings);
		        messageHelper.setSubject(String.format("Kiteable weather detected in %s", content.getLocatie()));
		        messageHelper.setText(String.format("", content.getLocatie(), content.getWindsnelheid(), content.getEenheidWindsnelheid(), content.getGolfhoogte(), content.getEenheidGolfhoogte(), content.getWindrichting(), content.getEenheidWindrichting(), content.getTijdstip()));
		    };
		    
		    emailSender.send(messagePreparator);
	    }
	    
	}

	private static String[] collectEmailAddresses(String location, Connection connection) {
		
		List<String> emailaddresses = new ArrayList<>();
		
		try {
			Statement statement = connection.createStatement();
			statement.execute(String.format("SELECT Email FROM %s", location));
			ResultSet resultSet = statement.getResultSet();
			
		    while (resultSet.next()) {
		    	emailaddresses.add(resultSet.getString(1));
		    }
			
		    
		    return emailaddresses.stream().toArray(String[]::new);
		    
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private static void insertDataIntoDatabase(GenericRecord value, Connection connection) {
		String dataID = value.get("DataID").toString();
		String loc = value.get("Locatie").toString();
		String windspeed = value.get("Windsnelheid").toString();
		String windspeedUnit = value.get("EenheidWindsnelheid").toString();
		String waveheight = value.get("Golfhoogte").toString();
		String waveheightUnit = value.get("EenheidGolfhoogte").toString();
		String winddirection = value.get("Windrichting").toString();
		String winddirectionUnit = value.get("EenheidWindrichting").toString();
		long timestamp = (long) value.get("Tijdstip");
		
		PreparedStatement st;
		try {
			st = connection.prepareStatement("INSERT INTO Kiten "
											+ "(DataID, Loc, "
											+ "Windspeed, WindspeedUnit, "
											+ "Waveheight, WaveheightUnit, "
											+ "Winddirection, WinddirectionUnit, "
											+ "TimestampMeasurment) "
											+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
											);
			st.setString(1, dataID);
			st.setString(2, loc);
			st.setString(3, windspeed);
			st.setString(4, windspeedUnit);
			st.setString(5, waveheight);
			st.setString(6, waveheightUnit);
			st.setString(7, winddirection);
			st.setString(8, winddirectionUnit);
			st.setLong(9, timestamp);
			st.executeUpdate();
			st.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		};
	}
	
    public void stopConsumer() {
    	closed.set(true);
        consumer.close();
    }
	
	public static SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetectedSerde() {
		final SpecificAvroSerde<KiteableWeatherDetected> kiteableWeatherDetected = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = new HashMap<>();
		serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, System.getenv(SCHEMA_REGISTRY_URL_CONFIG));
		kiteableWeatherDetected.configure(serdeConfig, false);
		return kiteableWeatherDetected;
	}
    
	private static JavaMailSender getMailingProps() {
		
		JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
		
		mailSender.setHost("mailhog");
		mailSender.setPort(1025);
		
		Properties props = mailSender.getJavaMailProperties();
	    props.put("mail.transport.protocol", "smtp");
	    props.put("mail.smtp.auth", "false");
	    props.put("mail.smtp.starttls.enable", "false");
	    props.put("mail.debug", "false");
		
		return mailSender;
	}
}
