package com.stage.adapter.mvb.database;

import java.util.Optional;

import org.postgresql.ds.PGPoolingDataSource;

public class Database {

	private PGPoolingDataSource source;
	
	public Database() {
		source = new PGPoolingDataSource();
	}
	
	public PGPoolingDataSource getSource() {
		return this.source;
	}
	
	public void createConnectionPool() {
		source.setDataSourceName(Optional.ofNullable(System.getenv("DATASOURCE_NAME")).orElseThrow(() -> new IllegalArgumentException("DATABASE_USER is required"))); // KiteableWeather
		source.setServerName(Optional.ofNullable(System.getenv("DATABASE_SERVER_NAME")).orElseThrow(() -> new IllegalArgumentException("DATABASE_USER is required")));	// localhost
		source.setDatabaseName(Optional.ofNullable(System.getenv("DATABASE_NAME")).orElseThrow(() -> new IllegalArgumentException("DATABASE_USER is required")));	// Stageopdracht
		source.setUser(Optional.ofNullable(System.getenv("DATABASE_USER")).orElseThrow(() -> new IllegalArgumentException("DATABASE_USER is required")));
		source.setPassword(Optional.ofNullable(System.getenv("DATABASE_PASSWORD")).orElseThrow(() -> new IllegalArgumentException("DATABASE_PASSWORD is required")));
		source.setMaxConnections(5);
	}
	
}
