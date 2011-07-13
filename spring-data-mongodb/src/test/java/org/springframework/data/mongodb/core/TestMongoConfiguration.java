package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.List;
import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

public class TestMongoConfiguration extends AbstractMongoConfiguration {

	@Override
	public String getDatabaseName() {
		return "database";
	}

	@Override
	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("localhost", 27017);
	}

	@Override
	public String getMappingBasePackage() {
		return "org.springframework.data.mongodb.core.core.mapping";
	}

	@Override
	protected void afterMappingMongoConverterCreation(MappingMongoConverter converter) {
		List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
		converters.add(new org.springframework.data.mongodb.core.PersonReadConverter());
		converters.add(new org.springframework.data.mongodb.core.PersonWriteConverter());
		converter.setCustomConversions(new CustomConversions(converters));
	}


}
