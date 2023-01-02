/*
 * Copyright 2017-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

import com.mongodb.client.MongoClient;

public class TestMongoConfiguration extends AbstractMongoClientConfiguration {

	@Override
	public String getDatabaseName() {
		return "database";
	}

	@Primary
	@Bean
	@Override
	public MappingMongoConverter mappingMongoConverter(MongoDatabaseFactory databaseFactory,
			MongoCustomConversions customConversions, MongoMappingContext mappingContext) {
		return super.mappingMongoConverter(databaseFactory, customConversions, mappingContext);
	}

	@Override
	@Bean
	public MongoClient mongoClient() {
		return MongoTestUtils.client();
	}

	@Override
	protected Collection<String> getMappingBasePackages() {
		return Collections.singleton(MongoMappingContext.class.getPackage().getName());
	}

	@Override
	public MongoCustomConversions customConversions() {

		List<Converter<?, ?>> converters = new ArrayList<>(2);
		converters.add(new org.springframework.data.mongodb.core.PersonReadConverter());
		converters.add(new org.springframework.data.mongodb.core.PersonWriteConverter());
		return new MongoCustomConversions(converters);
	}

	@Override
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		return Collections.emptySet();
	}
}
