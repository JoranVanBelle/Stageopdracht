package com.stage.adapter.mvb.producers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import com.stage.adapter.mvb.helpers.Bearertoken;

public class CurrentData implements Runnable {

	private static final int FETCH_API = 1000 * 60 * 60 * 1;	// ms * s * min * h
	private final String api;
	private String currentData;
	
	public CurrentData(String api) {
		this.api = api;
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
			String token = Bearertoken.getBearerToken(this.api);
			String response = fetchApi(String.format("%s/V2/currentData", this.api), token);
			System.out.println("Current data retrieved");
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return apiResponse;
		
	}
	
}
