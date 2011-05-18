package org.springframework.data.document.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;

import com.mongodb.Mongo;

public class TestMongoConfiguration extends AbstractMongoConfiguration {

	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("localhost", 27017);
	}

	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		return new MongoTemplate(mongo(), "database", mappingMongoConverter());
	}

	@Override
	public String getMappingBasePackage() {
		return "org.springframework.data.document.mongodb.mapping";
	}

	@Override
	protected void afterMappingMongoConverterCreation(
			MappingMongoConverter converter) {
		super.afterMappingMongoConverterCreation(converter);
		List<Converter<?, ?>> converterList = new ArrayList<Converter<?, ?>>();
		converterList.add(new org.springframework.data.document.mongodb.PersonReadConverter());
		converterList.add(new org.springframework.data.document.mongodb.PersonWriteConverter());
		converter.setCustomConverters(converterList);
	}
	
	

}
