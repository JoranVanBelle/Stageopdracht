package com.stage.adapter.mvb.database;

import org.testcontainers.utility.DockerImageName;

public interface PostgreSQLTestImages {
	
	DockerImageName POSTGRES_TEST_IMAGE = DockerImageName.parse("postgres:12");
}
