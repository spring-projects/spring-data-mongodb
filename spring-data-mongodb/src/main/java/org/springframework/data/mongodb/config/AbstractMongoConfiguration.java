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

import java.util.Collection;
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
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.support.CachingIsNewStrategyFactory;
import org.springframework.data.support.IsNewStrategyFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

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
	 * Returns the base packages to scan for MongoDB mapped entities at startup. Will return the package name of the
	 * configuration class' (the concrete class, not this one here) by default. So if you have a
	 * {@code com.acme.AppConfig} extending {@link AbstractMongoConfiguration} the base package will be considered
	 * {@code com.acme} unless the method is overridden to implement alternate behavior.
	 * 
	 * @return the base packages to scan for mapped {@link Document} classes or an empty collection to not enable scanning
	 *         for entities.
	 * @since 1.10
	 */
	protected Collection<String> getMappingBasePackages() {
		return Collections.singleton(getMappingBasePackage());
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
		mappingContext.setFieldNamingStrategy(fieldNamingStrategy());

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
	 * Scans the mapping base package for classes annotated with {@link Document}. By default, it scans for entities in
	 * all packages returned by {@link #getMappingBasePackages()}.
	 * 
	 * @see #getMappingBasePackages()
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		for (String basePackage : getMappingBasePackages()) {
			initialEntitySet.addAll(scanForEntities(basePackage));
		}

		return initialEntitySet;
	}

	/**
	 * Scans the given base package for entities, i.e. MongoDB specific types annotated with {@link Document} and
	 * {@link Persistent}.
	 * 
	 * @param basePackage must not be {@literal null}.
	 * @return
	 * @throws ClassNotFoundException
	 * @since 1.10
	 */
	protected Set<Class<?>> scanForEntities(String basePackage) throws ClassNotFoundException {

		if (!StringUtils.hasText(basePackage)) {
			return Collections.emptySet();
		}

		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {

			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {

				initialEntitySet
						.add(ClassUtils.forName(candidate.getBeanClassName(), AbstractMongoConfiguration.class.getClassLoader()));
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

	/**
	 * Configures a {@link FieldNamingStrategy} on the {@link MongoMappingContext} instance created.
	 * 
	 * @return
	 * @since 1.5
	 */
	protected FieldNamingStrategy fieldNamingStrategy() {
		return abbreviateFieldNames() ? new CamelCaseAbbreviatingFieldNamingStrategy()
				: PropertyNameFieldNamingStrategy.INSTANCE;
	}
}
