package org.springframework.data.mongodb.core.mapping;

import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;

public class GeoIndexedAppConfig extends AbstractMongoConfiguration {

	public static String GEO_DB = "database";
	public static String GEO_COLLECTION = "geolocation";

	@Override
	public String getDatabaseName() {
		return GEO_DB;
	}

	@Override
	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("127.0.0.1");
	}

	@Override
	public String getMappingBasePackage() {
		return "org.springframework.data.mongodb.core.core.mapping";
	}
	
  @Bean
  public LoggingEventListener<MongoMappingEvent<?>> mappingEventsListener() {
    return new LoggingEventListener<MongoMappingEvent<?>>();
  }
}
