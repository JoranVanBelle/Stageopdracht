package com.stage.adapter.mvb.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.testcontainers.containers.JdbcDatabaseContainer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class AbstractContainerDatabaseTest {
	
	  protected static ResultSet performQuery(JdbcDatabaseContainer<?> container, String sql) throws SQLException {
	        DataSource ds = getDataSource(container);
	        Statement statement = ds.getConnection().createStatement();
	        statement.execute(sql);
	        ResultSet resultSet = statement.getResultSet();
	        
	        if(!sql.contains("DELETE") && !sql.contains("INSERT INTO")) {
	        	resultSet.next();
	        }
	        
	        return resultSet;
	    }
	  
	  
	  
	  	protected Connection getConnection(JdbcDatabaseContainer<?> container) {
	        try {
	        	DataSource ds = getDataSource(container);
				return ds.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	        
	        throw new IllegalArgumentException("Something went wrong while retrieving a connection");
	  	}
	  
	    protected static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
	    	
	        HikariConfig hikariConfig = new HikariConfig();
	        hikariConfig.setJdbcUrl(container.getJdbcUrl());
	        hikariConfig.setUsername(container.getUsername());
	        hikariConfig.setPassword(container.getPassword());
	        hikariConfig.setDriverClassName(container.getDriverClassName());
	        hikariConfig.setMaximumPoolSize(2);
	        return new HikariDataSource(hikariConfig);
	    }

}
