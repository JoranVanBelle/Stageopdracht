package com.stage.adapter.mvb.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import org.json.JSONObject;

public class Bearertoken {
	
	public static String getBearerToken(String api, String username, String password) {		
		try {
			URL url = new URL(String.format("%s/token", api));
			
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			
			con.setRequestMethod("POST");
			
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("User-Agent", "Java client");
            
            String body = String.format("grant_type=password&username=%s&password=%s", username, password);
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            con.setDoOutput(true);
            
            try(OutputStream os = con.getOutputStream()) {
                byte[] input = body.getBytes("utf-8");
                os.write(input, 0, input.length);			
            } catch(UnknownHostException e) {
            	throw new RuntimeException("Invalid URL: " + url, e);
            }
            
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputline;
            StringBuilder response = new StringBuilder();
            while((inputline = in.readLine()) != null) {
            	response.append(inputline);
            }
            
            in.close();
            
            JSONObject responseJson = new JSONObject(response.toString());
            String access_token = responseJson.getString("access_token");
            return access_token;
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return "Something went wrong";
		} catch (IOException e) {
			e.printStackTrace();
			return "Something went wrong";
		}
	}
}
