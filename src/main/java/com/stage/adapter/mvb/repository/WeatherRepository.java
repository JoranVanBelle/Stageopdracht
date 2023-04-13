package com.stage.adapter.mvb.repository;

import org.apache.avro.generic.GenericRecord;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class WeatherRepository {
	
	private final NamedParameterJdbcTemplate jdbcTemplate;
	
	public WeatherRepository(NamedParameterJdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	public int insertDataIntoDatabase(GenericRecord value) {
		
		MapSqlParameterSource paramSource = new MapSqlParameterSource();
		paramSource.addValue("DataID", value.get("DataID").toString());
		paramSource.addValue("Location", value.get("Locatie").toString());
		paramSource.addValue("Windspeed", value.get("Windsnelheid").toString());
		paramSource.addValue("WindspeedUnit", value.get("EenheidWindsnelheid").toString());
		paramSource.addValue("Waveheight", value.get("Golfhoogte").toString());
		paramSource.addValue("WaveheightUnit", value.get("EenheidGolfhoogte").toString());
		paramSource.addValue("Winddirection", value.get("Windrichting").toString());
		paramSource.addValue("WinddirectionUnit", value.get("EenheidWindrichting").toString());
		paramSource.addValue("Timestamp", Long.parseLong(value.get("Tijdstip").toString()));
		
		int rowsAffected = jdbcTemplate.update(
				"INSERT INTO Kiten(DataID, Loc, Windspeed, WindspeedUnit, waveheight, waveheightUnit, winddirection, winddirectionUnit, TimestampMeasurment)"
				+ "VALUES (:DataID, :Location, :Windspeed, :WindspeedUnit, :Waveheight, :WaveheightUnit, :Winddirection, :WinddirectionUnit, :Timestamp)"
				, paramSource);
		
		return rowsAffected;
	}
}
