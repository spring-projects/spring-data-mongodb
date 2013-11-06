/*
 * Copyright 2011-2013 the original author or authors.
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mapping.context.MappingContextIsNewStrategyFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.support.CachingIsNewStrategyFactory;
import org.springframework.data.support.IsNewStrategyFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.mongodb.Mongo;

/**
 * Base class for Spring Data MongoDB configuration using JavaConfig.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Configuration
public abstract class AbstractMongoConfiguration {

	/**
	 * Return the name of the database to connect to.
	 * 
	 * @return must not be {@literal null}.
	 */
	protected abstract String getDatabaseName();

	/**
	 * Return the name of the authentication database to use.
	 * 
	 * @return must not be {@literal null}.
	 */
	protected String getAuthenticationDatabaseName() {
		return getDatabaseName();
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
	public SimpleMongoDbFactory mongoDbFactory() throws Exception {

		UserCredentials credentials = getUserCredentials();

		if (credentials == null) {
			return new SimpleMongoDbFactory(mongo(), getDatabaseName());
		} else {
			return new SimpleMongoDbFactory(mongo(), getDatabaseName(), credentials, getAuthenticationDatabaseName());
		}
	}

	/**
	 * Return the base package to scan for mapped {@link Document}s. Will return the package name of the configuration
	 * class' (the concrete class, not this one here) by default. So if you have a {@code com.acme.AppConfig} extending
	 * {@link AbstractMongoConfiguration} the base package will be considered {@code com.acme} unless the method is
	 * overriden to implement alternate behaviour.
	 * 
	 * @return the base package to scan for mapped {@link Document} classes or {@literal null} to not enable scanning for
	 *         entities.
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Return {@link UserCredentials} to be used when connecting to the MongoDB instance or {@literal null} if none shall
	 * be used.
	 * 
	 * @return
	 */
	protected UserCredentials getUserCredentials() {
		return null;
	}

	/**
	 * Creates a {@link MongoMappingContext} equipped with entity classes scanned from the mapping base package.
	 * 
	 * @see #getMappingBasePackage()
	 * @return
	 * @throws ClassNotFoundException
	 */
	@Bean
	public MongoMappingContext mongoMappingContext() throws ClassNotFoundException {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(getInitialEntitySet());
		mappingContext.setSimpleTypeHolder(customConversions().getSimpleTypeHolder());

		if (abbreviateFieldNames()) {
			mappingContext.setFieldNamingStrategy(new CamelCaseAbbreviatingFieldNamingStrategy());
		}

		return mappingContext;
	}

	/**
	 * Returns a {@link MappingContextIsNewStrategyFactory} wrapped into a {@link CachingIsNewStrategyFactory}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 */
	@Bean
	public IsNewStrategyFactory isNewStrategyFactory() throws ClassNotFoundException {
		return new CachingIsNewStrategyFactory(new MappingContextIsNewStrategyFactory(mongoMappingContext()));
	}

	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the {@link #mappingMongoConverter()} and
	 * {@link #mongoMappingContext()}. Returns an empty {@link CustomConversions} instance by default.
	 * 
	 * @return must not be {@literal null}.
	 */
	@Bean
	public CustomConversions customConversions() {
		return new CustomConversions(Collections.emptyList());
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

	/**
	 * Scans the mapping base package for classes annotated with {@link Document}.
	 * 
	 * @see #getMappingBasePackage()
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(),
						AbstractMongoConfiguration.class.getClassLoader()));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Configures whether to abbreviate field names for domain objects by configuring a
	 * {@link CamelCaseAbbreviatingFieldNamingStrategy} on the {@link MongoMappingContext} instance created. For advanced
	 * customization needs, consider overriding {@link #mappingMongoConverter()}.
	 * 
	 * @return
	 */
	protected boolean abbreviateFieldNames() {
		return false;
	}
}
