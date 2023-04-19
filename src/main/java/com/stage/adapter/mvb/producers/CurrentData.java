package com.stage.adapter.mvb.producers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.stage.adapter.mvb.helpers.Bearertoken;

public class CurrentData extends Thread {

	private static final int FETCH_API = 1000 * 60 * 60 * 1;	// ms * s * min * h
	private final String api;
	private final String username;
	private final String password;
	private String currentData;
	private static final Logger logger = LogManager.getLogger(CurrentData.class);
	
	public CurrentData(String api, String username, String password) {
		this.api = api;
		this.username = username;
		this.password = password;
	}
	
	public String getCurrentDataString() {
		return this.currentData;
	}
	
	public JSONObject getCurrentData() {
		return new JSONObject("{ current data: [" + this.currentData.substring(1, this.currentData.length() - 1) + "]}");
	}
	
	private void setCurrentData(String currentData) {
		this.currentData = currentData;
	}

	@Override
	public void run() {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(() -> {
			String token = Bearertoken.getBearerToken(api, username, password);
			String response = fetchApi(String.format("%s/V2/currentData", api), token);
			System.out.println("ℹ️ Current data retrieved");
			setCurrentData(response);

		}, 0, FETCH_API, TimeUnit.MILLISECONDS);
	}
	
	private String fetchApi(String api, String token) {
		
		String apiResponse = "";
		
		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request;
		try {
			request = HttpRequest.newBuilder()
					.GET()
					.uri(new URI(api))
					.header("Authorization", String.format("bearer %s", token))
					.build();
			
			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			apiResponse = response.body();
		} catch (URISyntaxException e) {
			logger.error("❌", e);
		} catch (IOException e) {
			logger.error("❌", e);
		} catch (InterruptedException e) {
			logger.error("❌", e);
		}
		
		return apiResponse;
		
	}
	
}
