package org.springframework.data.document.mongodb.mapping;

import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.document.mongodb.mapping.event.LoggingEventListener;
import org.springframework.data.document.mongodb.mapping.event.MongoMappingEvent;

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
		return new Mongo("localhost");
	}

	@Override
	public String getMappingBasePackage() {
		return "org.springframework.data.document.mongodb.mapping";
	}
	
  @Bean
  public LoggingEventListener<MongoMappingEvent<?>> mappingEventsListener() {
    return new LoggingEventListener<MongoMappingEvent<?>>();
  }
}
