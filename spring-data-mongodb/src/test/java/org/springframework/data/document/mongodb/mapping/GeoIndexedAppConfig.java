package org.springframework.data.document.mongodb.mapping;

import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.data.document.mongodb.MongoDbFactory;
import org.springframework.data.document.mongodb.MongoDbFactoryBean;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;

public class GeoIndexedAppConfig extends AbstractMongoConfiguration {

	public static String GEO_DB = "geodb";
	public static String GEO_COLLECTION = "geolocation";

	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("localhost");
	}

	@Bean
	public MongoDbFactory mongoDbFactory() throws Exception {
		return new MongoDbFactoryBean(mongo(), GEO_DB);
	}

	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		return new MongoTemplate(mongoDbFactory(), mappingMongoConverter());
	}

	public String getMappingBasePackage() {
		return "org.springframework.data.document.mongodb.mapping";
	}

}
