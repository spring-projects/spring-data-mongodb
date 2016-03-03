/*
 * Copyright 2011-2016 the original author or authors.
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
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Base class for Spring Data MongoDB configuration using JavaConfig.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Ryan Tenney
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see MongoConfigurationSupport
 */
@Configuration
public abstract class AbstractMongoConfiguration extends MongoConfigurationSupport {

	/**
	 * Return the name of the authentication database to use. Defaults to {@literal null} and will turn into the value
	 * returned by {@link #getDatabaseName()} later on effectively.
	 * 
	 * @return
	 * @deprecated since 1.7. {@link MongoClient} should hold authentication data within
	 *             {@link MongoClient#getCredentialsList()}
	 */
	@Deprecated
	protected String getAuthenticationDatabaseName() {
		return null;
	}

	/**
	 * Return the {@link Mongo} instance to connect to. Annotate with {@link Bean} in case you want to expose a
	 * {@link Mongo} instance to the {@link org.springframework.context.ApplicationContext}.
	 * 
	 * @return
	 * @throws Exception
	 */
	public abstract Mongo mongo() throws Exception;

	/**
	 * Creates a {@link MongoTemplate}.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		return new MongoTemplate(mongoDbFactory(), mappingMongoConverter());
	}

	/**
	 * Creates a {@link SimpleMongoDbFactory} to be used by the {@link MongoTemplate}. Will use the {@link Mongo} instance
	 * configured in {@link #mongo()}.
	 * 
	 * @see #mongo()
	 * @see #mongoTemplate()
	 * @return
	 * @throws Exception
	 */
	@Bean
	public MongoDbFactory mongoDbFactory() throws Exception {
		return new SimpleMongoDbFactory(mongo(), getDatabaseName(), getUserCredentials(), getAuthenticationDatabaseName());
	}

	/**
	 * Return the base package to scan for mapped {@link Document}s. Will return the package name of the configuration
	 * class' (the concrete class, not this one here) by default. So if you have a {@code com.acme.AppConfig} extending
	 * {@link AbstractMongoConfiguration} the base package will be considered {@code com.acme} unless the method is
	 * overridden to implement alternate behavior.
	 *
	 * @return the base package to scan for mapped {@link Document} classes or {@literal null} to not enable scanning for
	 *         entities.
	 * @deprecated use {@link #getMappingBasePackages()} instead.
	 */
	@Deprecated
	protected String getMappingBasePackage() {

		Package mappingBasePackage = getClass().getPackage();
		return mappingBasePackage == null ? null : mappingBasePackage.getName();
	}

	/**
	 * Return {@link UserCredentials} to be used when connecting to the MongoDB instance or {@literal null} if none shall
	 * be used.
	 * 
	 * @return
	 * @deprecated since 1.7. {@link MongoClient} should hold authentication data within
	 *             {@link MongoClient#getCredentialsList()}
	 */
	@Deprecated
	protected UserCredentials getUserCredentials() {
		return null;
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

		return converter;
	}
}
