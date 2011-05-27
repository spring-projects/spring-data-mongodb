package org.springframework.data.document.mongodb;

import java.util.HashSet;
import java.util.Set;

import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.document.mongodb.convert.MappingMongoConverter;

public class TestMongoConfiguration extends AbstractMongoConfiguration {

	@Override
	public String getDatabaseName() {
		return "database";
	}

	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("localhost", 27017);
	}

	@Override
	public String getMappingBasePackage() {
		return "org.springframework.data.document.mongodb.mapping";
	}

	@Override
	protected void afterMappingMongoConverterCreation(MappingMongoConverter converter) {
		Set<Converter<?, ?>> converterList = new HashSet<Converter<?, ?>>();
		converterList.add(new org.springframework.data.document.mongodb.PersonReadConverter());
		converterList.add(new org.springframework.data.document.mongodb.PersonWriteConverter());
		converter.setCustomConverters(converterList);
	}


}
