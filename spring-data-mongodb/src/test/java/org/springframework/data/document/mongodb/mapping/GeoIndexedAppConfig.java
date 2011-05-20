package org.springframework.data.document.mongodb.mapping;

import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;

public class GeoIndexedAppConfig extends AbstractMongoConfiguration {

	public static String GEO_DB = "database";
	public static String GEO_COLLECTION = "geolocation";

	@Override
	public String defaultDatabaseName() {
		return GEO_DB;
	}

	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("localhost");
	}

	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		return new MongoTemplate(mongoDbFactory());
	}

	public String getMappingBasePackage() {
		return "org.springframework.data.document.mongodb.mapping";
	}

}
