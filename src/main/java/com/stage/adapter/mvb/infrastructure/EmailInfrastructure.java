package com.stage.adapter.mvb.infrastructure;

import java.util.List;
import java.util.Properties;

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import com.stage.KiteableWeatherDetected;

public class EmailInfrastructure {


	private String username;
	private String password;

	private final Properties prop;

	public EmailInfrastructure(String host, int port, String username, String password) {
		prop = new Properties();
		prop.put("mail.smtp.auth", true);
		prop.put("mail.smtp.starttls.enable", "true");
		prop.put("mail.smtp.host", host);
		prop.put("mail.smtp.port", port);
		prop.put("mail.smtp.ssl.trust", host);

		this.username = username;
		this.password = password;
	}

	public EmailInfrastructure(String host, int port) {
		prop = new Properties();
		prop.put("mail.smtp.host", host);
		prop.put("mail.smtp.port", port);
	}

	public void sendEmail(KiteableWeatherDetected content, List<String> emails, String baseUrl) {

		Session session = Session.getInstance(prop, new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		Message message = new MimeMessage(session);
		Multipart multipart = new MimeMultipart();
		try {
			message.setFrom(new InternetAddress("joran.vanbelle2@student.hogent.be"));
			for(String email : emails) {
				message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(email));

				MimeBodyPart mimeBodyPart = new MimeBodyPart();
				mimeBodyPart.setContent(getText(content), "text/html; charset=utf-8");
				multipart.addBodyPart(mimeBodyPart);

				MimeBodyPart unsubPart = new MimeBodyPart();
				unsubPart.setContent(String.format("<p>Click <a href=\"%s/%s/%s\">here</a> to unsubscribe from this email.</p>", "text/html; charset=utf-8", baseUrl, email, content.getLocatie()), "text/html; charset=utf-8");
				multipart.addBodyPart(unsubPart);

				message.setContent(multipart);
				message.setSubject(String.format("Kiteable weather detected at %s", content.getLocatie()));

				Transport.send(message);
			}

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getText(KiteableWeatherDetected weather) {
		return String.format(
				"Kiteable weather detected at %s\r\n"
						+ "Windspeed: %s %s\r\n"
						+ "Waveheight: %s %s\r\n"
						+ "Winddirection: %s %s\r\n",
				weather.getLocatie(),
				weather.getWindsnelheid(),
				weather.getEenheidWindsnelheid(),
				weather.getGolfhoogte(),
				weather.getEenheidGolfhoogte(),
				weather.getWindrichting(),
				weather.getEenheidWindrichting(),
				weather.getTijdstip());
	}
}
