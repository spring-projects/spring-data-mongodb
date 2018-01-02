/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Base class for reactive Spring Data MongoDB configuration using JavaConfig.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see MongoConfigurationSupport
 */
@Configuration
public abstract class AbstractReactiveMongoConfiguration extends MongoConfigurationSupport {

	/**
	 * Return the Reactive Streams {@link MongoClient} instance to connect to. Annotate with {@link Bean} in case you want
	 * to expose a {@link MongoClient} instance to the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return never {@literal null}.
	 */
	public abstract MongoClient reactiveMongoClient();

	/**
	 * Creates {@link ReactiveMongoOperations}.
	 *
	 * @return never {@literal null}.
	 */
	@Bean
	public ReactiveMongoOperations reactiveMongoTemplate() throws Exception {
		return new ReactiveMongoTemplate(reactiveMongoDbFactory(), mappingMongoConverter());
	}

	/**
	 * Creates a {@link ReactiveMongoDatabaseFactory} to be used by the {@link ReactiveMongoOperations}. Will use the
	 * {@link MongoClient} instance configured in {@link #reactiveMongoClient()}.
	 *
	 * @see #reactiveMongoClient()
	 * @see #reactiveMongoTemplate()
	 * @return never {@literal null}.
	 */
	@Bean
	public ReactiveMongoDatabaseFactory reactiveMongoDbFactory() {
		return new SimpleReactiveMongoDatabaseFactory(reactiveMongoClient(), getDatabaseName());
	}

	/**
	 * Creates a {@link MappingMongoConverter} using the configured {@link #reactiveMongoDbFactory()} and
	 * {@link #mongoMappingContext()}. Will get {@link #customConversions()} applied.
	 *
	 * @see #customConversions()
	 * @see #mongoMappingContext()
	 * @see #reactiveMongoDbFactory()
	 * @return never {@literal null}.
	 * @throws Exception
	 */
	@Bean
	public MappingMongoConverter mappingMongoConverter() throws Exception {

		MappingMongoConverter converter = new MappingMongoConverter(ReactiveMongoTemplate.NO_OP_REF_RESOLVER,
				mongoMappingContext());
		converter.setCustomConversions(customConversions());

		return converter;
	}
}
