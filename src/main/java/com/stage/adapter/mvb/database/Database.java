package com.stage.adapter.mvb.database;

import org.postgresql.ds.PGSimpleDataSource;

public class Database {

	private PGSimpleDataSource source;
	
	public Database() {
		source = new PGSimpleDataSource(); 
	}
	
	public PGSimpleDataSource getPGPoolingDataSource(String database_url, String database_user, String database_password) {
		source.setDatabaseName(database_url);
		source.setUser(database_user);
		source.setPassword(database_password);
		System.err.println("Database connection created");
		return source;
	}
	
}
