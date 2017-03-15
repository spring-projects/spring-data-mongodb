package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public class TestMongoConfiguration extends AbstractMongoConfiguration {

	@Override
	public String getDatabaseName() {
		return "database";
	}

	@Override
	@Bean
	public MongoClient mongoClient() throws Exception {
		return new MongoClient("127.0.0.1", 27017);
	}

	@Override
	public String getMappingBasePackage() {
		return MongoMappingContext.class.getPackage().getName();
	}

	@Override
	public CustomConversions customConversions() {

		List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
		converters.add(new org.springframework.data.mongodb.core.PersonReadConverter());
		converters.add(new org.springframework.data.mongodb.core.PersonWriteConverter());
		return new CustomConversions(converters);
	}
}
