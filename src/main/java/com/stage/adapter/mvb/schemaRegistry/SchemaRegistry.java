package com.stage.adapter.mvb.schemaRegistry;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class SchemaRegistry {
	
	public static void main(String[] args) {
		String url = "/producer_api/src/main/resources/data";
		String registry = "http://localhost:8081";
		Map<String, String> configs = new HashMap<>();
		configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "url");
		SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(registry, 99);
		
//		String path = "../../../../../../resources/data";
		String path = "src/main/resources/data";
		File dir = new File(path);
		File[] files = dir.listFiles();
		System.out.println(dir);
		
		for(File file : files) {
			Schema schema;
			AvroSchema avroSchema;
			try {
				System.out.printf("%s wordt geüpload naar schema registry: ", file.getName());
				schema = new Schema.Parser().parse(file);
				avroSchema = new AvroSchema(schema);
				schemaRegistryClient.register("_mvbschemas", avroSchema);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
}
