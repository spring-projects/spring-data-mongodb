/*
 * Copyright 2018-2019 the original author or authors.
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
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.lang.Nullable;

import com.mongodb.client.MongoClient;

/**
 * Base class for Spring Data MongoDB configuration using JavaConfig with {@link com.mongodb.client.MongoClient}.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @see MongoConfigurationSupport
 */
@Configuration
public abstract class AbstractMongoClientConfiguration extends MongoConfigurationSupport {

	/**
	 * Return the {@link MongoClient} instance to connect to. Annotate with {@link Bean} in case you want to expose a
	 * {@link MongoClient} instance to the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return
	 */
	public abstract MongoClient mongoClient();

	/**
	 * Creates a {@link MongoTemplate}.
	 *
	 * @return
	 */
	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		return new MongoTemplate(mongoDbFactory(), mappingMongoConverter());
	}

	/**
	 * Creates a {@link org.springframework.data.mongodb.core.SimpleMongoDbFactory;} to be used by the
	 * {@link MongoTemplate}. Will use the {@link MongoClient} instance configured in {@link #mongoClient()}.
	 *
	 * @see #mongoClient()
	 * @see #mongoTemplate()
	 * @return
	 */
	@Bean
	public MongoDbFactory mongoDbFactory() {
		return new SimpleMongoClientDbFactory(mongoClient(), getDatabaseName());
	}

	/**
	 * Return the base package to scan for mapped {@link Document}s. Will return the package name of the configuration
	 * class' (the concrete class, not this one here) by default. So if you have a {@code com.acme.AppConfig} extending
	 * {@link AbstractMongoClientConfiguration} the base package will be considered {@code com.acme} unless the method is
	 * overridden to implement alternate behavior.
	 *
	 * @return the base package to scan for mapped {@link Document} classes or {@literal null} to not enable scanning for
	 *         entities.
	 * @deprecated use {@link #getMappingBasePackages()} instead.
	 */
	@Deprecated
	@Nullable
	protected String getMappingBasePackage() {

		Package mappingBasePackage = getClass().getPackage();
		return mappingBasePackage == null ? null : mappingBasePackage.getName();
	}

	/**
	 * Creates a {@link MappingMongoConverter} using the configured {@link #mongoDbFactory()} and
	 * {@link #mongoMappingContext()}. Will get {@link #customConversions()} applied.
	 *
	 * @see #customConversions()
	 * @see #mongoMappingContext()
	 * @see #mongoDbFactory()
	 * @return
	 * @throws Exception
	 */
	@Bean
	public MappingMongoConverter mappingMongoConverter() throws Exception {

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDbFactory());
		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mongoMappingContext());
		converter.setCustomConversions(customConversions());
		converter.setCodecRegistryProvider(mongoDbFactory());

		return converter;
	}
}
