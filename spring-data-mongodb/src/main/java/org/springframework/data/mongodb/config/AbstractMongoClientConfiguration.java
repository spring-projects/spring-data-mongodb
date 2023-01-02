/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.SpringDataMongoDB;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Base class for Spring Data MongoDB configuration using JavaConfig with {@link com.mongodb.client.MongoClient}.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @see MongoConfigurationSupport
 */
@Configuration(proxyBeanMethods = false)
public abstract class AbstractMongoClientConfiguration extends MongoConfigurationSupport {

	/**
	 * Return the {@link MongoClient} instance to connect to. Annotate with {@link Bean} in case you want to expose a
	 * {@link MongoClient} instance to the {@link org.springframework.context.ApplicationContext}. <br />
	 * Override {@link #mongoClientSettings()} to configure connection details.
	 *
	 * @return never {@literal null}.
	 * @see #mongoClientSettings()
	 * @see #configureClientSettings(Builder)
	 */
	public MongoClient mongoClient() {
		return createMongoClient(mongoClientSettings());
	}

	/**
	 * Creates a {@link MongoTemplate}.
	 *
	 * @see #mongoDbFactory()
	 * @see #mappingMongoConverter(MongoDatabaseFactory, MongoCustomConversions, MongoMappingContext)
	 */
	@Bean
	public MongoTemplate mongoTemplate(MongoDatabaseFactory databaseFactory, MappingMongoConverter converter) {
		return new MongoTemplate(databaseFactory, converter);
	}

	/**
	 * Creates a {@link org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory} to be used by the
	 * {@link MongoTemplate}. Will use the {@link MongoClient} instance configured in {@link #mongoClient()}.
	 *
	 * @see #mongoClient()
	 * @see #mongoTemplate(MongoDatabaseFactory, MappingMongoConverter)
	 */
	@Bean
	public MongoDatabaseFactory mongoDbFactory() {
		return new SimpleMongoClientDatabaseFactory(mongoClient(), getDatabaseName());
	}

	/**
	 * Creates a {@link MappingMongoConverter} using the configured {@link #mongoDbFactory()} and
	 * {@link #mongoMappingContext(MongoCustomConversions, org.springframework.data.mongodb.MongoManagedTypes)}. Will get {@link #customConversions()} applied.
	 *
	 * @see #customConversions()
	 * @see #mongoMappingContext(MongoCustomConversions, org.springframework.data.mongodb.MongoManagedTypes)
	 * @see #mongoDbFactory()
	 */
	@Bean
	public MappingMongoConverter mappingMongoConverter(MongoDatabaseFactory databaseFactory,
			MongoCustomConversions customConversions, MongoMappingContext mappingContext) {

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(databaseFactory);
		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		converter.setCustomConversions(customConversions);
		converter.setCodecRegistryProvider(databaseFactory);

		return converter;
	}

	/**
	 * Create the Reactive Streams {@link com.mongodb.reactivestreams.client.MongoClient} instance with given
	 * {@link MongoClientSettings}.
	 *
	 * @return never {@literal null}.
	 * @since 3.0
	 */
	protected MongoClient createMongoClient(MongoClientSettings settings) {
		return MongoClients.create(settings, SpringDataMongoDB.driverInformation());
	}
}
