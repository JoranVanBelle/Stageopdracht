package com.stage.adapter.mvb.database;

import java.util.Optional;

import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import jakarta.activation.DataSource;

public class Database {

	private PGSimpleDataSource source;
	
	public Database() {
		source = new PGSimpleDataSource(); 
	}
	
	public PGSimpleDataSource getPGPoolingDataSource() {
		source.setDatabaseName(Optional.ofNullable(System.getenv("DATABASE_URL")).orElseThrow(() -> new IllegalArgumentException("DATABASE_URL is required")));
		source.setUser(Optional.ofNullable(System.getenv("DATABASE_USER")).orElseThrow(() -> new IllegalArgumentException("DATABASE_USER is required")));
		source.setPassword(Optional.ofNullable(System.getenv("DATABASE_PASSWORD")).orElseThrow(() -> new IllegalArgumentException("DATABASE_PASSWORD is required")));
	
		return source;
	}
	
}
