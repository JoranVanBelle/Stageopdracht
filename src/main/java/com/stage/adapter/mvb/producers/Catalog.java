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

import org.json.JSONObject;

import com.stage.adapter.mvb.helpers.Bearertoken;

public class Catalog implements Runnable {

	private static final int FETCH_API = 1000 * 60 * 60 * 168;	// ms * s * min * h - 1x in de 7d
	private final String api;
	private String catalog;
	
	public Catalog(String api) {
		this.api = api;
	}
	
	public String getCatalogString() {
		return this.catalog;
	}
	
	public JSONObject getCatalog() {
		return new JSONObject(this.catalog);
	}
	
	private void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	
	@Override
	public void run() {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		scheduler.scheduleAtFixedRate(() -> {
			String token = Bearertoken.getBearerToken(this.api);
			String response = fetchApi(String.format("%s/V2/catalog", this.api), token);
			System.out.println("Catalog retrieved");
			setCatalog(response);
			
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
